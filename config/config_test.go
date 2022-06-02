package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p-core/network"
	rcmgr "github.com/libp2p/go-libp2p-resource-manager"
	"github.com/stretchr/testify/assert"
)

func checkNodeConfig(t *testing.T, node *Node) {
	assert := assert.New(t)
	assert.NotEmpty(node.Blockstore)
	assert.NotEmpty(node.Libp2pKeyFile)
	assert.NotEmpty(node.DatastoreDir)
	assert.NotEmpty(node.WalletDir)

	assert.Greater(node.Limits.SystemLimit.Conns, 0)
	assert.Greater(node.Limits.SystemLimit.ConnsInbound, 0)
	assert.Greater(node.Limits.SystemLimit.ConnsOutbound, 0)
	assert.Greater(node.Limits.SystemLimit.Streams, 0)
	assert.Greater(node.Limits.SystemLimit.StreamsInbound, 0)
	assert.Greater(node.Limits.SystemLimit.StreamsOutbound, 0)
	assert.Greater(node.Limits.SystemLimit.FD, 0)
	assert.Greater(node.Limits.SystemLimit.MinMemory, int64(0))
	assert.Greater(node.Limits.SystemLimit.MaxMemory, node.Limits.SystemLimit.MinMemory)

	assert.Greater(node.Limits.TransientLimit.Conns, 0)
	assert.Greater(node.Limits.TransientLimit.ConnsInbound, 0)
	assert.Greater(node.Limits.TransientLimit.ConnsOutbound, 0)
	assert.Greater(node.Limits.TransientLimit.Streams, 0)
	assert.Greater(node.Limits.TransientLimit.StreamsInbound, 0)
	assert.Greater(node.Limits.TransientLimit.StreamsOutbound, 0)
	assert.Greater(node.Limits.TransientLimit.FD, 0)

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

	limiter := rcmgr.NewDefaultLimiter()
	config.apply(limiter)

	assert.Equal(limiter.SystemLimits.GetConnLimit(network.DirInbound), config.SystemLimit.ConnsInbound)
	assert.Equal(limiter.SystemLimits.GetConnLimit(network.DirOutbound), config.SystemLimit.ConnsOutbound)
	assert.Equal(limiter.SystemLimits.GetConnTotalLimit(), config.SystemLimit.Conns)
	assert.Equal(limiter.TransientLimits.GetConnLimit(network.DirInbound), config.TransientLimit.ConnsInbound)
	assert.Equal(limiter.TransientLimits.GetConnLimit(network.DirOutbound), config.TransientLimit.ConnsOutbound)
	assert.Equal(limiter.TransientLimits.GetConnTotalLimit(), config.TransientLimit.Conns)
	assert.Equal(limiter.SystemLimits.GetStreamLimit(network.DirInbound), config.SystemLimit.StreamsInbound)
	assert.Equal(limiter.SystemLimits.GetStreamLimit(network.DirOutbound), config.SystemLimit.StreamsOutbound)
	assert.Equal(limiter.SystemLimits.GetStreamTotalLimit(), config.SystemLimit.Streams)
	assert.Equal(limiter.TransientLimits.GetStreamLimit(network.DirInbound), config.TransientLimit.StreamsInbound)
	assert.Equal(limiter.TransientLimits.GetStreamLimit(network.DirOutbound), config.TransientLimit.StreamsOutbound)
	assert.Equal(limiter.TransientLimits.GetStreamTotalLimit(), config.TransientLimit.Streams)
	assert.Equal(limiter.SystemLimits.GetFDLimit(), config.SystemLimit.FD)
	assert.Equal(limiter.TransientLimits.GetFDLimit(), config.TransientLimit.FD)
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
