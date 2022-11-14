package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/stretchr/testify/assert"
)

func checkNodeConfig(t *testing.T, node *Node) {
	assert := assert.New(t)
	assert.NotEmpty(node.Blockstore)
	assert.NotEmpty(node.Libp2pKeyFile)
	assert.NotEmpty(node.DatastoreDir)
	assert.NotEmpty(node.WalletDir)

	assert.Greater(node.Limits.SystemBaseLimit.Conns, 0)
	assert.Greater(node.Limits.SystemBaseLimit.ConnsInbound, 0)
	assert.Greater(node.Limits.SystemBaseLimit.ConnsOutbound, 0)
	assert.Greater(node.Limits.SystemBaseLimit.Streams, 0)
	assert.Greater(node.Limits.SystemBaseLimit.StreamsInbound, 0)
	assert.Greater(node.Limits.SystemBaseLimit.StreamsOutbound, 0)
	assert.Greater(node.Limits.SystemBaseLimit.FD, 0)
	assert.Greater(node.Limits.SystemBaseLimit.Memory, int64(0))

	assert.Greater(node.Limits.TransientBaseLimit.Conns, 0)
	assert.Greater(node.Limits.TransientBaseLimit.ConnsInbound, 0)
	assert.Greater(node.Limits.TransientBaseLimit.ConnsOutbound, 0)
	assert.Greater(node.Limits.TransientBaseLimit.Streams, 0)
	assert.Greater(node.Limits.TransientBaseLimit.StreamsInbound, 0)
	assert.Greater(node.Limits.TransientBaseLimit.StreamsOutbound, 0)
	assert.Greater(node.Limits.TransientBaseLimit.FD, 0)

	assert.NotEmpty(node.ListenAddrs)
	assert.NotEmpty(node.ListenAddrs[0])

	assert.Greater(node.ConnectionManager.LowWater, 0)
	assert.Greater(node.ConnectionManager.HighWater, node.ConnectionManager.LowWater)
}

func TestEstuaryDefaultSanity(t *testing.T) {

	assert := assert.New(t)
	config := NewEstuary("test-version")
	config.SetRequiredOptions()

	assert.NotEmpty(config.DataDir)
	assert.NotEmpty(config.StagingDataDir)
	assert.NotEmpty(config.DatabaseConnString)
	assert.NotEmpty(config.ApiListen)
	assert.NotEmpty(config.Hostname)

	checkNodeConfig(t, &config.Node)
}

func TestShuttleDefaultSanity(t *testing.T) {

	assert := assert.New(t)
	config := NewShuttle("test-version")
	config.SetRequiredOptions()

	assert.NotEmpty(config.DataDir)
	assert.NotEmpty(config.StagingDataDir)
	assert.NotEmpty(config.DatabaseConnString)
	assert.NotEmpty(config.ApiListen)
	assert.NotEmpty(config.EstuaryRemote.Api)

	checkNodeConfig(t, &config.Node)
}

func TestApplyLimits(t *testing.T) {
	assert := assert.New(t)
	config := NewShuttle("test-version").Node.Limits

	limiter := rcmgr.NewFixedLimiter(config.AutoScale())
	systemLimits := limiter.GetSystemLimits()
	transientLimits := limiter.GetTransientLimits()

	assert.Equal(systemLimits.GetConnLimit(network.DirInbound), config.SystemBaseLimit.ConnsInbound)
	assert.Equal(systemLimits.GetConnLimit(network.DirOutbound), config.SystemBaseLimit.ConnsOutbound)
	assert.Equal(systemLimits.GetConnTotalLimit(), config.SystemBaseLimit.Conns)
	assert.Equal(transientLimits.GetConnLimit(network.DirInbound), config.TransientBaseLimit.ConnsInbound)
	assert.Equal(transientLimits.GetConnLimit(network.DirOutbound), config.TransientBaseLimit.ConnsOutbound)
	assert.Equal(transientLimits.GetConnTotalLimit(), config.TransientBaseLimit.Conns)
	assert.Equal(systemLimits.GetStreamLimit(network.DirInbound), config.SystemBaseLimit.StreamsInbound)
	assert.Equal(systemLimits.GetStreamLimit(network.DirOutbound), config.SystemBaseLimit.StreamsOutbound)
	assert.Equal(systemLimits.GetStreamTotalLimit(), config.SystemBaseLimit.Streams)
	assert.Equal(transientLimits.GetStreamLimit(network.DirInbound), config.TransientBaseLimit.StreamsInbound)
	assert.Equal(transientLimits.GetStreamLimit(network.DirOutbound), config.TransientBaseLimit.StreamsOutbound)
	assert.Equal(transientLimits.GetStreamTotalLimit(), config.TransientBaseLimit.Streams)
	assert.Equal(systemLimits.GetFDLimit(), config.SystemBaseLimit.FD)
	assert.Equal(transientLimits.GetFDLimit(), config.TransientBaseLimit.FD)
}

func TestEstuaryJSONRoundtrip(t *testing.T) {
	assert := assert.New(t)
	config := NewEstuary("test-version")
	path := filepath.Join(os.TempDir(), "test.json")
	save(config, path)
	config2 := Estuary{}
	load(&config2, path)
	assert.Equal(config, &config2)
}

func TestShuttleJSONRoundtrip(t *testing.T) {
	assert := assert.New(t)
	config := NewShuttle("test-version")
	path := filepath.Join(os.TempDir(), "test.json")
	save(config, path)
	config2 := Shuttle{}
	load(&config2, path)
	assert.Equal(config, &config2)
}
