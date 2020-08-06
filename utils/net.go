package utils

import (
	"context"
	"os"

	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"github.com/testground/sdk-go/network"
)

// SetupNetwork instructs the sidecar (if enabled) to setup the network for this
// test case.
func SetupNetwork(ctx context.Context, runenv *runtime.RunEnv, client *network.Client,
	nodetp NodeType, tpindex int) (error) {

	if !runenv.TestSidecar {
		return nil
	}

	// Wait for the network to be initialized.
	if err := client.WaitNetworkInitialized(ctx); err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	// Reject or accept connections based on availability
	var action network.FilterAction
	if nodetp == Unavailable {
		action = network.Reject
	} else {
		action = network.Accept
	}

	cfg := &network.Config{
		Network: "default",
		Enable:  nodetp != Unavailable,
		Default: network.LinkShape{
			Filter: action,
		},
		CallbackState: sync.State("configured" + hostname),
		CallbackTarget: 1,
	}

	client.MustConfigureNetwork(ctx, cfg)
	return nil
}