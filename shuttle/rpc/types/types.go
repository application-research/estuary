package types

import (
	rcpevent "github.com/application-research/estuary/shuttle/rpc/event"
)

type MessageHandlerFn func(msg *rcpevent.Message, source string) error
type CommandHandlerFn func(msg *rcpevent.Command, source string) error
