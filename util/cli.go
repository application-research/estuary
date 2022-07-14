package util

import cli "github.com/urfave/cli/v2"

var LogLevl string

var FlagLogLevl = &cli.StringFlag{
	Name:        "log-level",
	Usage:       "sets the log level, defaults to INFO",
	Value:       "INFO",
	Destination: &LogLevl,
}
