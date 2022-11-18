package util

import "github.com/urfave/cli/v2"

var LogLevel string

var FlagLogLevel = &cli.StringFlag{
	Name:        "log-level",
	Usage:       "sets the log level, defaults to INFO",
	Value:       "INFO",
	Destination: &LogLevel,
}
