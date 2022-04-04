package config

import rcmgr "github.com/libp2p/go-libp2p-resource-manager"

type SystemLimit struct {
	MinMemory      int64
	MaxMemory      int64
	MemoryFraction float64

	StreamsInbound  int
	StreamsOutbound int
	Streams         int

	ConnsInbound  int
	ConnsOutbound int
	Conns         int

	FD int
}

func (sl *SystemLimit) apply(lim *rcmgr.BasicLimiter) {
	lim.SystemLimits = lim.SystemLimits.WithFDLimit(sl.FD).WithConnLimit(sl.ConnsInbound, sl.ConnsOutbound, sl.Conns).WithStreamLimit(sl.StreamsInbound, sl.StreamsOutbound, sl.Streams).WithMemoryLimit(sl.MemoryFraction, sl.MinMemory, sl.MaxMemory)
}

type TransientLimit struct {
	StreamsInbound  int
	StreamsOutbound int
	Streams         int

	ConnsInbound  int
	ConnsOutbound int
	Conns         int

	FD int
}

func (tl *TransientLimit) apply(lim *rcmgr.BasicLimiter) {
	lim.TransientLimits = lim.TransientLimits.WithFDLimit(tl.FD).WithConnLimit(tl.ConnsInbound, tl.ConnsOutbound, tl.Conns).WithStreamLimit(tl.StreamsInbound, tl.StreamsOutbound, tl.Streams)
}

type Limits struct {
	SystemLimitConfig    SystemLimit
	TransientLimitConfig TransientLimit
}

func (limits *Limits) apply(lim *rcmgr.BasicLimiter) {
	limits.SystemLimitConfig.apply(lim)
	limits.TransientLimitConfig.apply(lim)
}
