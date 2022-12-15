package queue

import (
	"fmt"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/shuttle/rpc/engines/queue/drivers/nsq"
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/shuttle/rpc/types"

	"go.uber.org/zap"
)

type IEstuaryRpcEngine interface {
	SendMessage(topic string, handle string, msg *rpcevent.Command) error
}

type IShuttleRpcEngine interface {
	SendMessage(topic string, msg *rpcevent.Message) error
}

func NewEstuaryRpcEngine(cfg *config.Estuary, log *zap.SugaredLogger, handlerFn types.MessageHandlerFn) (IEstuaryRpcEngine, error) {
	if handlerFn == nil {
		return nil, fmt.Errorf("command handler is required")
	}

	if cfg.RpcEngine.Queue.Host == "" {
		return nil, fmt.Errorf("queue host is required")
	}

	if cfg.RpcEngine.Queue.Consumers == 0 {
		return nil, fmt.Errorf("queue consumers cannot be 0")
	}

	if cfg.RpcEngine.Queue.Driver == nsq.Driver {
		log.Debugf("going to use %s queue for rpc....", cfg.RpcEngine.Queue.Driver)
		return nsq.NewEstuaryRpcQueue(cfg, log, handlerFn)
	}
	return nil, fmt.Errorf("%s queue driver is not supported", cfg.RpcEngine.Queue.Driver)
}

func NewShuttleRpcEngine(cfg *config.Shuttle, handle string, log *zap.SugaredLogger, handlerFn types.CommandHandlerFn) (IShuttleRpcEngine, error) {
	if handlerFn == nil {
		return nil, fmt.Errorf("command handler is required")
	}

	if handle == "" {
		return nil, fmt.Errorf("shuttle handle is required")
	}

	if cfg.RpcEngine.Queue.Host == "" {
		return nil, fmt.Errorf("queue host is required")
	}

	if cfg.RpcEngine.Queue.Consumers == 0 {
		return nil, fmt.Errorf("queue consumers cannot be 0")
	}

	if cfg.RpcEngine.Queue.Driver == nsq.Driver {
		log.Debugf("going to use %s queue for rpc....", cfg.RpcEngine.Queue.Driver)
		return nsq.NewShuttleRpcQueue(cfg, log, handle, handlerFn)
	}
	return nil, fmt.Errorf("%s queue driver is not supported", cfg.RpcEngine.Queue.Driver)
}
