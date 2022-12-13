package config

type RpcEngine struct {
	Queue     QueueEngine     `json:"queue"`
	Websocket WebsocketEngine `json:"websocket"`
}

type QueueEngine struct {
	Host      string `json:"host"`
	Consumers int    `json:"consumers"`
	Enabled   bool   `json:"enabled"`
	Driver    string `json:"driver"`
}

type WebsocketEngine struct {
	IncomingQueueSize int `json:"incoming_queue_size"`
	OutgoingQueueSize int `json:"outgoing_queue_size"`
	QueueHandlers     int `json:"queue_handlers"`
}
