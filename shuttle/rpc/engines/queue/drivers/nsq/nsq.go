package nsq

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/application-research/estuary/config"
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/shuttle/rpc/types"

	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
)

const Driver = "nsq"

type EstuaryRpcQueue struct {
	producer *nsq.Producer
}

type ShuttleRpcQueue struct {
	producer *nsq.Producer
}

func NewEstuaryRpcQueue(cfg *config.Estuary, log *zap.SugaredLogger, handlerFn types.MessageHandlerFn) (*EstuaryRpcQueue, error) {
	prd, err := nsq.NewProducer(cfg.RpcEngine.Queue.Host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	if err := registerEstuaryConsumers(log, cfg, handlerFn); err != nil {
		return nil, err
	}
	return &EstuaryRpcQueue{producer: prd}, nil
}

func NewShuttleRpcQueue(cfg *config.Shuttle, log *zap.SugaredLogger, handle string, handlerFn types.CommandHandlerFn) (*ShuttleRpcQueue, error) {
	prd, err := nsq.NewProducer(cfg.RpcEngine.Queue.Host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	if err := registerShuttleConsumers(cfg, log, handle, handlerFn); err != nil {
		return nil, err
	}
	return &ShuttleRpcQueue{producer: prd}, nil
}

func registerEstuaryConsumers(log *zap.SugaredLogger, cfg *config.Estuary, handlerFn types.MessageHandlerFn) error {
	config := nsq.NewConfig()

	for topic := range rpcevent.MessageTopics {
		for i := 1; i <= cfg.RpcEngine.Queue.Consumers; i++ {
			log.Debugf("registering estuary consumer#%d for topic: %s", i, topic)

			q, err := nsq.NewConsumer(topic, "main", config)
			if err != nil {
				return err
			}

			q.AddHandler(nsq.HandlerFunc(func(nsqMsg *nsq.Message) error {
				var msg *rpcevent.Message
				if err := json.Unmarshal(nsqMsg.Body, &msg); err != nil {
					log.Errorf("failed to parse message with error: %s", err)
				}

				if err := handlerFn(msg, "queue"); err != nil {
					log.Errorf("failed to process message: %s with error: %s", msg.Op, err)
				} else {
					nsqMsg.Finish()
				}
				return nil
			}))

			if err := q.ConnectToNSQD(cfg.RpcEngine.Queue.Host); err != nil {
				return err
			}
		}
	}
	return nil
}

func registerShuttleConsumers(cfg *config.Shuttle, log *zap.SugaredLogger, handle string, handlerFn types.CommandHandlerFn) error {
	config := nsq.NewConfig()

	for topic := range rpcevent.CommandTopics {
		for i := 1; i <= cfg.RpcEngine.Queue.Consumers; i++ {
			ntopic := getHandleTopic(topic, handle)
			log.Debugf("registering shuttle consumer#%d for topic: %s", i, ntopic)

			q, err := nsq.NewConsumer(ntopic, "main", config)
			if err != nil {
				return err
			}

			q.AddHandler(nsq.HandlerFunc(func(nsqMsg *nsq.Message) error {
				var msg *rpcevent.Command
				if err := json.Unmarshal(nsqMsg.Body, &msg); err != nil {
					log.Errorf("failed to parse command with error: %s", err)
				}

				if err := handlerFn(msg, "queue"); err != nil {
					log.Errorf("failed to process command with error: %s", err)
				} else {
					nsqMsg.Finish()
				}
				return nil
			}))

			if err := q.ConnectToNSQD(cfg.RpcEngine.Queue.Host); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *EstuaryRpcQueue) SendMessage(topic string, handle string, msg *rpcevent.Command) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	topic = getHandleTopic(topic, handle)
	return m.producer.Publish(topic, b)
}

func (m *ShuttleRpcQueue) SendMessage(topic string, msg *rpcevent.Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return m.producer.Publish(topic, b)
}

// nsq topics has a 64 character lenght limit, so strip out "HANDLE" and "SHUTTLE"
// also join topic_handle so estuary can send/recieves messages for a particular shuttle
// and have dedicated consumers for each topic
func getHandleTopic(topic, handle string) string {
	topic = fmt.Sprintf("%s_%s", topic, handle)
	return strings.Replace(strings.TrimSuffix(topic, "HANDLE"), "SHUTTLE", "", 1)
}
