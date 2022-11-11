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

	assert.Greater(node.LimitConfig.System.Conns, 0)
	assert.Greater(node.LimitConfig.System.ConnsInbound, 0)
	assert.Greater(node.LimitConfig.System.ConnsOutbound, 0)
	assert.Greater(node.LimitConfig.System.Streams, 0)
	assert.Greater(node.LimitConfig.System.StreamsInbound, 0)
	assert.Greater(node.LimitConfig.System.StreamsOutbound, 0)
	assert.Greater(node.LimitConfig.System.FD, 0)
	assert.Greater(node.LimitConfig.System.Memory, int64(0))

	assert.Greater(node.LimitConfig.Transient.Conns, 0)
	assert.Greater(node.LimitConfig.Transient.ConnsInbound, 0)
	assert.Greater(node.LimitConfig.Transient.ConnsOutbound, 0)
	assert.Greater(node.LimitConfig.Transient.Streams, 0)
	assert.Greater(node.LimitConfig.Transient.StreamsInbound, 0)
	assert.Greater(node.LimitConfig.Transient.StreamsOutbound, 0)
	assert.Greater(node.LimitConfig.Transient.FD, 0)

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
	config := NewShuttle("test-version").Node.LimitConfig

	limiter := rcmgr.NewFixedLimiter(config)
	systemLimits := limiter.GetSystemLimits()
	transientLimits := limiter.GetTransientLimits()

	assert.Equal(systemLimits.GetConnLimit(network.DirInbound), config.System.ConnsInbound)
	assert.Equal(systemLimits.GetConnLimit(network.DirOutbound), config.System.ConnsOutbound)
	assert.Equal(systemLimits.GetConnTotalLimit(), config.System.Conns)
	assert.Equal(transientLimits.GetConnLimit(network.DirInbound), config.Transient.ConnsInbound)
	assert.Equal(transientLimits.GetConnLimit(network.DirOutbound), config.Transient.ConnsOutbound)
	assert.Equal(transientLimits.GetConnTotalLimit(), config.Transient.Conns)
	assert.Equal(systemLimits.GetStreamLimit(network.DirInbound), config.System.StreamsInbound)
	assert.Equal(systemLimits.GetStreamLimit(network.DirOutbound), config.System.StreamsOutbound)
	assert.Equal(systemLimits.GetStreamTotalLimit(), config.System.Streams)
	assert.Equal(transientLimits.GetStreamLimit(network.DirInbound), config.Transient.StreamsInbound)
	assert.Equal(transientLimits.GetStreamLimit(network.DirOutbound), config.Transient.StreamsOutbound)
	assert.Equal(transientLimits.GetStreamTotalLimit(), config.Transient.Streams)
	assert.Equal(systemLimits.GetFDLimit(), config.System.FD)
	assert.Equal(transientLimits.GetFDLimit(), config.Transient.FD)
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
