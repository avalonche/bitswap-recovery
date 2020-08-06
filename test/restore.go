package test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"
	"errors"
	
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"

	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"github.com/testground/sdk-go/network"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/avalonche/bitswap-restore/utils"
)

// Restore tests for different recovery methods
func Restore(runenv *runtime.RunEnv) error {
	// Test params
	timeout := time.Duration(runenv.IntParam("timeout_secs"))
	fileSize := runenv.IntParam("file_size")
	unavailableCounts, err := utils.ParseIntArray(runenv.StringParam("unavailable_count"))
	if err != nil {
		return err
	}

	// Set up
	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()

	client := sync.MustBoundClient(ctx, runenv)
	netclient := network.NewClient(client, runenv)
	netclient.MustWaitNetworkInitialized(ctx)

	// Tear down
	defer func() {
		client.Close()
	}()

	// New libp2p node
	h, err := libp2p.New(ctx)
	if err != nil {
		return err
	}
	defer h.Close()
	runenv.RecordMessage("This is host %s with addrs: %v", h.ID(), h.Addrs())

	peers := sync.NewTopic("peers", &peer.AddrInfo{})

	// Get sequence number
	seqNum, err := client.Publish(ctx, peers, host.InfoFromHost(h))
	if err != nil {
		return err
	}

	// Index of nodes that contain file data, which are all nodes except seq 1 (Leech Node)
	seedIndex := seqNum - 2

	// Get addresses of peers
	peerCh := make(chan *peer.AddrInfo)
	pctx, cancelSub := context.WithCancel(ctx)
	_, err = client.Subscribe(pctx, peers, peerCh)
	if err != nil {
		cancelSub()
		return err
	}
	addrInfos, err := utils.AddrInfosFromChan(peerCh, runenv.TestInstanceCount)
	if err != nil {
		cancelSub()
		return err
	}
	cancelSub()

	var rootCid cid.Cid

	for runIdx, unavailableCount := range unavailableCounts {
		// Reset timeout for each run of unavailable counts
		ctx, cancel := context.WithTimeout(ctx, timeout*time.Second)
		defer cancel()
		
		// Flag for whether the file fetch failed
		failedRetrieval := false

		// Setup the type of node the instance will be
		grpseq, nodetp, tpindex, err := parseInfo(ctx, runenv, client, h, seqNum, unavailableCount)
		if err != nil {
			return err
		}
		// Setup network for some unavailable
		err = utils.SetupNetwork(ctx, runenv, netclient, nodetp, tpindex)
		if err != nil {
			return fmt.Errorf("Network setup failed: %w", err)
		}

		// Ready to start run
		runID := fmt.Sprintf("%d-unavailable", unavailableCount)
		client.MustSignalAndWait(ctx, sync.State("start-run-" + runID), runenv.TestInstanceCount)

		runenv.RecordMessage("Starting run with %d unavailable out of %d seeds (file size - %d bytes)", unavailableCount, runenv.TestInstanceCount - 1, fileSize)
		rootCidTopic := sync.NewTopic("root-cid", &cid.Cid{})

		// New blockstore
		bstore, err := utils.CreateBlockstore(ctx)
		if err != nil {
			return err
		}

		// New bitswap node from blockstore
		bsnode, err := utils.CreateBitswapNode(ctx, h, bstore)
		if err != nil {
			return err 
		}

		switch nodetp {
		case utils.Leech:
			// First run, get root cid from seed
			if runIdx == 0 {
				rootCidCh := make(chan *cid.Cid, 1)
				sctx, cancelRootCidSub := context.WithCancel(ctx)
				_, err = client.Subscribe(sctx, rootCidTopic, rootCidCh)
				if err != nil {
					cancelRootCidSub()
					return fmt.Errorf("Failed to subscribe to rootCidTopic %w", err)
				}
				// Get rootCid from one seed
				rootCidPtr, ok := <- rootCidCh
				cancelRootCidSub()
				if !ok {
					return fmt.Errorf("Getting root cid timed out")
				}
				rootCid = *rootCidPtr
			}
		case utils.Seed:
			seedGenerated := sync.State("seed-generated-" + runID)
			// Avoid overloading machine when generating data
			// allow each seed generate data sequentially
			if err != nil {
				return err
			}
			if seedIndex > 0 {
				doneCh := client.MustBarrier(ctx, seedGenerated, tpindex).C
				if err = <-doneCh; err != nil {
					return err
				}
			}
			runenv.RecordMessage("Generating seed data of %d bytes", fileSize)
			rootCid, err := setupData(ctx, runenv, bsnode, fileSize, int(seedIndex))
			if err != nil {
				return fmt.Errorf("Failed to set up file: %w", err)
			}
			runenv.RecordMessage("Done generating seed data of %d bytes", fileSize)

			// Signal completion of seed generation
			_, err = client.SignalEntry(ctx, seedGenerated)
			if err != nil {
				return fmt.Errorf("Failed to signal seed generated: %w", err)
			}

			// Generate new root cid data on first run
			if runIdx == 0 {
				// Inform other nodes of root CID
				if _, err = client.Publish(ctx, rootCidTopic, &rootCid); err != nil {
					return fmt.Errorf("Failed to get Redis Sync rootCidTopic %w", err)
				}
			}
		}

		// Signal and wait to dial nodes
		client.SignalAndWait(ctx, sync.State("ready-to-connect-" + runID), runenv.TestInstanceCount)
		
		// Dial all peers, we expect some dials to fail
		if nodetp != utils.Unavailable {
			dialed, failedConnections, err := utils.DialOtherPeers(ctx, h, addrInfos, unavailableCount)
			if err != nil {
				return err
			}
			runenv.RecordMessage("Dialed %d other nodes, failed to connect with %d other nodes", len(dialed), failedConnections)
		}
		// Wait for all nodes to be connected
		client.SignalAndWait(ctx, sync.State("connect-complete-" + runID), runenv.TestInstanceCount - unavailableCount)

		// Start file transfer
		if nodetp == utils.Leech {
			origFile := utils.RandReader(fileSize)
			origBytes, err := ioutil.ReadAll(origFile)
			if err != nil {
				return fmt.Errorf("Error reading file: %w", err)
			}
			// Error when file can not be retrieved (data loss) exceeded specified time limit
			fctx, fCancel := context.WithTimeout(context.Background(), 10 * time.Second)
			defer fCancel()
			fetchedBytes, err := recoverData(fctx, runenv, bsnode, rootCid)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					runenv.RecordMessage("Could not fetch file within timeout")
					failedRetrieval = true
				} else {
					return fmt.Errorf("Error fetching bytes %w", err)
				}
			}
			
			if !failedRetrieval {
				err = arrComp(origBytes, fetchedBytes)
				if err != nil {
					return fmt.Errorf("Fetched data does not match the original file: %w", err)
				}
			}
			// Check that we have retrieved the original file
			runenv.RecordMessage("Leech fetch complete")
		}

		// Wait for all leeches to have downloaded the data from seeds
		client.SignalAndWait(ctx, sync.State("transfer-complete-" + runID), runenv.TestInstanceCount - unavailableCount)

		// Report metrics
		storageSize, err := utils.GetBlockstoreSize(ctx, bstore)
		if err != nil {
			return fmt.Errorf("Failed to get size of blockstore: %w", err)
		}
		err = emitMetrics(runenv, bsnode, runIdx, grpseq, fileSize, nodetp, tpindex, storageSize, failedRetrieval)
		if err != nil {
			return err
		}

		// Shut down bitswap
		err = bsnode.Close()
		if err != nil {
			return fmt.Errorf("Error closing Bitswap: %w", err)
		}

		// Disconnect peers
		for _, c := range h.Network().Conns() {
			err := c.Close()
			if err != nil {
				return fmt.Errorf("Error disconnecting: %w", err)
			}
		}

		if nodetp == utils.Leech {
			// Free up memory by clearing the leech blockstore at the end of each run.
			// Note that although we create a new blockstore for the leech at the
			// start of the run, explicitly cleaning up the blockstore from the
			// previous run allows it to be GCed.
			if err := utils.ClearBlockstore(ctx, bstore); err != nil {
				return fmt.Errorf("Error clearing blockstore: %w", err)
			}
		}

	}
	return nil
}

func parseInfo(ctx context.Context, runenv *runtime.RunEnv, client *sync.DefaultClient, h host.Host, seqNum int64, unavailableCount int) (int64, utils.NodeType, int, error) {
	var nodetp utils.NodeType
	var tpindex int
	// seq start at 1 instead of 0
	switch {
	case seqNum <= 1:
		// One node to request file
		nodetp = utils.Leech
		tpindex = int(seqNum) - 1
	case seqNum > 1 + int64(unavailableCount):
		nodetp = utils.Seed
		tpindex = int(seqNum) - 2 - unavailableCount
	default:
		nodetp = utils.Unavailable
		tpindex = int(seqNum) - 2
	}

	runenv.RecordMessage("This is %s %d", nodetp.String(), tpindex)
	return seqNum, nodetp, tpindex, nil
}

// TODO encoding and placement into DAG Service
func setupData(ctx context.Context, runenv *runtime.RunEnv, node *utils.Node, fileSize int, seedIndex int) (cid.Cid, error) {
	tmpFile := utils.RandReader(fileSize)
	switch runenv.TestCase {
	case "ipfs":
		return setupIPFSSeed(ctx, runenv, node, tmpFile, seedIndex)
	case "reed-solomon":
		// parse rs params and distribute ipld graph in dagservice
		// setupRS()
	case "entanglement":
		// parse entanglement params and distribute ipld dag in dagservice
		// setupEntanglement
	default:
		return cid.Cid{}, fmt.Errorf("Unrecognised test case")
	}
	return cid.Cid{}, fmt.Errorf("Unrecognised test case")
}

func setupIPFSSeed(ctx context.Context, runenv *runtime.RunEnv, node *utils.Node, tmpFile io.Reader, seedIndex int) (cid.Cid, error) {
	ipldNode, err := node.Add(ctx, tmpFile)
	if err != nil {
		return cid.Cid{}, err
	}

	replicationFactor := runenv.IntParam("replication_factor")
	// Default is full replication - all nodes contain all data blocks
	if replicationFactor <= 0 || replicationFactor >= runenv.TestInstanceCount {
		return ipldNode.Cid(), nil
	}

	numerator := replicationFactor
	denominator := runenv.TestInstanceCount

	// Get data blocks and remove some of the block to distribute between nodes
	nodes, err := getLeafNodes(ctx, ipldNode, node.Dserv)
	if err != nil {
		return cid.Cid{}, err
	}

	// TODO: improve block distribution among nodes
	var del []cid.Cid
	for i := 0; i < len(nodes); i++ {
		idx := i + seedIndex
		if idx%int(denominator) >= int(numerator) {
			del = append(del, nodes[i].Cid())
		}
	}
	
	if err := node.Dserv.RemoveMany(ctx, del); err != nil {
		return cid.Cid{}, err
	}

	runenv.RecordMessage("Removed %d / %d blocks, blockstore has %d / %d fraction of blocks remaining", len(del), len(nodes), numerator, denominator)
	return ipldNode.Cid(), nil
}

// func setupRS(ctx context.Context, runenv *runtime.RunEnv, node *utils.Node, tmpFile io.Reader, seedIndex int) (cid.Cid, error) {
// 	ipldNode, err := node.Add(ctx, tmpFile)
// 	if err != nil {
// 		return cid.Cid{}, err
// 	}
// 	// Level of recoverability
// 	parity := runenv.IntParam("parity")

// 	ng := merkledag.NewSession(ctx, n.Dserv)
// 	// Encoding the dag
// 	encodedNode, err := recovery.EncodeDAG()

// 	// Remove data blocks and distributed between nodes
// 	nodes, err := getLeafNodes(ctx, encodedNOde, node.Dserv)
// 	if err != nil {
// 		return cid.Cid{}, err
// 	}

// 	var del []cid.Cid
// 	for i := 0; i < len(nodes) * numerator; i++ {
// 		if i%denominator != numerator {
// 			del = append(del, nodes[i%len(nodes)].Cid())
// 		}
// 	}
	
// 	if err := node.Dserv.RemoveMany(ctx, del); err != nil {
// 		return cid.Cid{}, err
// 	}

// 	runenv.RecordMessage("Removed %d / %d blocks, blockstore has %d / %d fraction of blocks remaining", len(del), len(nodes), numerator, denominator)
// 	return encodedNode.Cid(), nil
// }

// func setupEntanglement() {}

func getLeafNodes(ctx context.Context, node ipld.Node, dserv ipld.DAGService) ([]ipld.Node, error) {
	if len(node.Links()) == 0 {
		return []ipld.Node{node}, nil
	}

	var leaves []ipld.Node
	for _, l := range node.Links() {
		child, err := l.GetNode(ctx, dserv)
		if err != nil {
			return nil, err
		}
		childLeaves, err := getLeafNodes(ctx, child, dserv)
		if err != nil {
			return nil, err
		}
		leaves = append(leaves, childLeaves...)
	}

	return leaves, nil
}

// TODO: Perform recovery
func recoverData(ctx context.Context, runenv *runtime.RunEnv, node *utils.Node, rootCid cid.Cid) ([]byte, error) {
	switch runenv.TestCase {
	case "ipfs":
		return ipfsFetchData(ctx, node, rootCid)
	case "reed-solomon":
		// rsFetchData()
	case "entanglement":
		// entangleFetchData()
	}
	return nil, fmt.Errorf("Unrecognised test case")
}

func ipfsFetchData(ctx context.Context, node *utils.Node, rootCid cid.Cid) ([]byte, error) {
	// Fetch DAG from peers
	err := node.FetchGraph(ctx, rootCid)
	if err != nil {
		return nil, fmt.Errorf("Error fetching data through Bitswap %w", err)
	}
	// Fetch file now in Leech blockstore
	n, err := node.Get(ctx, rootCid)
	if err != nil {
		return nil, fmt.Errorf("Unable to return UnixFS file")
	}
	
	fn, ok := n.(files.File)
	if !ok {
		return nil, fmt.Errorf("Could not convert node to file")
	}

	return ioutil.ReadAll(fn)
}

// func rsFetchData() {}
// func entangleFetchData() {}

func arrComp(a []byte, b []byte) error {
	if len(a) != len(b) {
		return fmt.Errorf("Byte arrays differ in length. %d != %d", len(a), len(b))
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return fmt.Errorf("Byte arrays differ at index: %d", i)
		}
	}
	return nil
}

func emitMetrics(runenv *runtime.RunEnv, bsnode *utils.Node, runIdx int, grpseq int64, filesize int, nodetp utils.NodeType,
	tpindex int, storageSize int, failedRetrieval bool) error {

	stats, err := bsnode.Bitswap.Stat()
	if err != nil {
		return fmt.Errorf("error getting stats from bitswap: %w", err)
	}

	id := fmt.Sprintf("run:%d/groupname:%s/groupseq:%d/filesize:%d/nodetype:%s/nodetypeindex:%d/failed:%t",
		runIdx, runenv.TestGroupID, grpseq, filesize, nodetp, tpindex, failedRetrieval)

	runenv.R().RecordPoint(fmt.Sprintf("%s/name:msgs_rcvd", id), float64(stats.MessagesReceived))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:data_sent", id), float64(stats.DataSent))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:data_rcvd", id), float64(stats.DataReceived))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:dup_data_rcvd", id), float64(stats.DupDataReceived))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:blks_sent", id), float64(stats.BlocksSent))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:blks_rcvd", id), float64(stats.BlocksReceived))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:dup_blks_rcvd", id), float64(stats.DupBlksReceived))
	runenv.R().RecordPoint(fmt.Sprintf("%s/name:data_stored", id), float64(storageSize))

	return nil
}
