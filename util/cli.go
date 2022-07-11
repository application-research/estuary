package util

import cli "github.com/urfave/cli/v2"

var IsVeryVerbose bool

var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "vv",
	Usage:       "enables very verbose mode, useful for debugging",
	Value:       false,
	Destination: &IsVeryVerbose,
}
