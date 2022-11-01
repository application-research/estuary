package config

import rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

type SystemLimit struct {
	MinMemory      int64   `json:"min_memory"`
	MaxMemory      int64   `json:"max_memory"`
	MemoryFraction float64 `json:"memory_fraction"`

	StreamsInbound  int `json:"streams_inbound"`
	StreamsOutbound int `json:"streams_outbound"`
	Streams         int `json:"streams"`

	ConnsInbound  int `json:"conns_inbound"`
	ConnsOutbound int `json:"conns_outbound"`
	Conns         int `json:"conns"`

	FD int `json:"fd"`
}

func (sl *SystemLimit) apply(lim *rcmgr.BasicLimiter) {
	lim.SystemLimits = lim.SystemLimits.WithFDLimit(sl.FD).WithConnLimit(sl.ConnsInbound, sl.ConnsOutbound, sl.Conns).WithStreamLimit(sl.StreamsInbound, sl.StreamsOutbound, sl.Streams).WithMemoryLimit(sl.MemoryFraction, sl.MinMemory, sl.MaxMemory)
}

type TransientLimit struct {
	StreamsInbound  int `json:"streams_inbound"`
	StreamsOutbound int `json:"streams_outbound"`
	Streams         int `json:"streams"`

	ConnsInbound  int `json:"conns_inbound"`
	ConnsOutbound int `json:"conns_outbound"`
	Conns         int `json:"conns"`

	FD int `json:"fd"`
}

func (tl *TransientLimit) apply(lim *rcmgr.BasicLimiter) {
	lim.TransientLimits = lim.TransientLimits.WithFDLimit(tl.FD).WithConnLimit(tl.ConnsInbound, tl.ConnsOutbound, tl.Conns).WithStreamLimit(tl.StreamsInbound, tl.StreamsOutbound, tl.Streams)
}

type Limits struct {
	SystemLimit    SystemLimit    `json:"system_limit"`
	TransientLimit TransientLimit `json:"transient_limit"`
}

func (limits *Limits) apply(lim *rcmgr.BasicLimiter) {
	limits.SystemLimit.apply(lim)
	limits.TransientLimit.apply(lim)
}
