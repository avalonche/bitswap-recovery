package main

import (
	test "github.com/avalonche/bitswap-restore/test"

	"github.com/testground/sdk-go/run"
)

func main() {
	run.Invoke(test.Restore)
}
