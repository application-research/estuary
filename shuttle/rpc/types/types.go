package types

import (
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
)

type MessageHandlerFn func(msg *rpcevent.Message, source string) error
type CommandHandlerFn func(msg *rpcevent.Command, source string) error
