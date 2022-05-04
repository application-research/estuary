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
	assert.NotEmpty(node.BlockstoreDir)
	assert.NotEmpty(node.Libp2pKeyFile)
	assert.NotEmpty(node.DatastoreDir)
	assert.NotEmpty(node.WalletDir)

	assert.Greater(node.LimitsConfig.SystemLimitConfig.Conns, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.ConnsInbound, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.ConnsOutbound, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.Streams, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.StreamsInbound, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.StreamsOutbound, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.FD, 0)
	assert.Greater(node.LimitsConfig.SystemLimitConfig.MinMemory, int64(0))
	assert.Greater(node.LimitsConfig.SystemLimitConfig.MaxMemory, node.LimitsConfig.SystemLimitConfig.MinMemory)

	assert.Greater(node.LimitsConfig.TransientLimitConfig.Conns, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.ConnsInbound, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.ConnsOutbound, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.Streams, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.StreamsInbound, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.StreamsOutbound, 0)
	assert.Greater(node.LimitsConfig.TransientLimitConfig.FD, 0)

	assert.NotEmpty(node.ListenAddrs)
	assert.NotEmpty(node.ListenAddrs[0])

	assert.Greater(node.ConnectionManagerConfig.LowWater, 0)
	assert.Greater(node.ConnectionManagerConfig.HighWater, node.ConnectionManagerConfig.LowWater)
}

func TestEstuaryDefaultSanity(t *testing.T) {

	assert := assert.New(t)
	config := NewEstuary()

	assert.NotEmpty(config.DataDir)
	assert.NotEmpty(config.StagingDataDir)
	assert.NotEmpty(config.DatabaseConnString)
	assert.NotEmpty(config.ApiListen)
	assert.NotEmpty(config.Hostname)

	checkNodeConfig(t, &config.NodeConfig)
}

func TestShuttleDefaultSanity(t *testing.T) {

	assert := assert.New(t)
	config := NewShuttle()

	assert.NotEmpty(config.DataDir)
	assert.NotEmpty(config.StagingDataDir)
	assert.NotEmpty(config.DatabaseConnString)
	assert.NotEmpty(config.ApiListen)
	assert.NotEmpty(config.EstuaryConfig.Api)

	checkNodeConfig(t, &config.NodeConfig)
}

func TestApplyLimits(t *testing.T) {
	assert := assert.New(t)
	config := NewShuttle().NodeConfig.LimitsConfig

	limiter := rcmgr.NewDefaultLimiter()
	config.apply(limiter)

	assert.Equal(limiter.SystemLimits.GetConnLimit(network.DirInbound), config.SystemLimitConfig.ConnsInbound)
	assert.Equal(limiter.SystemLimits.GetConnLimit(network.DirOutbound), config.SystemLimitConfig.ConnsOutbound)
	assert.Equal(limiter.SystemLimits.GetConnTotalLimit(), config.SystemLimitConfig.Conns)
	assert.Equal(limiter.TransientLimits.GetConnLimit(network.DirInbound), config.TransientLimitConfig.ConnsInbound)
	assert.Equal(limiter.TransientLimits.GetConnLimit(network.DirOutbound), config.TransientLimitConfig.ConnsOutbound)
	assert.Equal(limiter.TransientLimits.GetConnTotalLimit(), config.TransientLimitConfig.Conns)
	assert.Equal(limiter.SystemLimits.GetStreamLimit(network.DirInbound), config.SystemLimitConfig.StreamsInbound)
	assert.Equal(limiter.SystemLimits.GetStreamLimit(network.DirOutbound), config.SystemLimitConfig.StreamsOutbound)
	assert.Equal(limiter.SystemLimits.GetStreamTotalLimit(), config.SystemLimitConfig.Streams)
	assert.Equal(limiter.TransientLimits.GetStreamLimit(network.DirInbound), config.TransientLimitConfig.StreamsInbound)
	assert.Equal(limiter.TransientLimits.GetStreamLimit(network.DirOutbound), config.TransientLimitConfig.StreamsOutbound)
	assert.Equal(limiter.TransientLimits.GetStreamTotalLimit(), config.TransientLimitConfig.Streams)
	assert.Equal(limiter.SystemLimits.GetFDLimit(), config.SystemLimitConfig.FD)
	assert.Equal(limiter.TransientLimits.GetFDLimit(), config.TransientLimitConfig.FD)
}

func TestEstuaryJSONRoundtrip(t *testing.T) {
	assert := assert.New(t)
	config := NewEstuary()
	path := filepath.Join(os.TempDir(), "test.json")
	save(config, path)
	config2 := Estuary{}
	load(&config2, path)
	assert.Equal(config, &config2)
}

func TestShuttleJSONRoundtrip(t *testing.T) {
	assert := assert.New(t)
	config := NewShuttle()
	path := filepath.Join(os.TempDir(), "test.json")
	save(config, path)
	config2 := Shuttle{}
	load(&config2, path)
	assert.Equal(config, &config2)
}
