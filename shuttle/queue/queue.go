package queue

import (
	"encoding/json"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/drpc"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
)

type Emanager struct {
	cfg      *config.Estuary
	producer *nsq.Producer
	log      *zap.SugaredLogger
	hanldfn  func(msg *drpc.Message) error
}

type smanager struct {
	cfg      *config.Estuary
	producer *nsq.Producer
	log      *zap.SugaredLogger
	hanldfn  func(msg *drpc.Command) error
}

func NewEstuaryQueueMgr(cfg *config.Estuary, fn func(msg *drpc.Message) error) (*Emanager, error) {
	prd, err := nsq.NewProducer(cfg.RPCQueue.Host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	mn := &Emanager{
		cfg:      cfg,
		producer: prd,
		hanldfn:  fn,
	}
	if err := mn.registerConsumers(); err != nil {
		return nil, err
	}

	return &Emanager{
		cfg:      cfg,
		producer: prd,
		hanldfn:  fn,
	}, nil
}

func newShuttleQueueMgr(cfg *config.Estuary) (*Emanager, error) {
	prd, err := nsq.NewProducer(cfg.RPCQueue.Host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	return &Emanager{
		cfg:      cfg,
		producer: prd,
	}, nil
}

func (m *Emanager) registerConsumers() error {
	config := nsq.NewConfig()

	for topic := range drpc.OPtopics {
		q, err := nsq.NewConsumer(topic, "main", config)
		if err != nil {
			return err
		}

		q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			var msg *drpc.Message
			if err := json.Unmarshal(message.Body, &msg); err != nil {
				m.log.Errorf("failed to process message with error: %s", err)
			}
			return nil
		}))

		return q.ConnectToNSQLookupd(m.cfg.RPCQueue.Host)
	}
	return nil
	// if err := q.ConnectToNSQD(m.cfg.RPCQueue.Host); err != nil {
}

func (m *Emanager) Produce(topic string, msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return m.producer.Publish(topic, b)
}

func (m *smanager) Produce(msg *drpc.Message) error {
	return m.producer.Publish("write_test", []byte("test"))
}
