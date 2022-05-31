package config

type Bitswap struct {
	MaxOutstandingBytesPerPeer int64 `json:"max_outstanding_bytes_per_peer"`
	TargetMessageSize          int   `json:"target_message_size"`
}
