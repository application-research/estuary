package config

type RPCMessage struct {
	IncomingQueueSize int `json:"incoming_queue_size"`
	OutgoingQueueSize int `json:"outgoing_queue_size"`
	QueueHandlers     int `json:"queue_handlers"`
}
