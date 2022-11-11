package main

import (
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/libp2p/go-libp2p/core/metrics"
)

func StartBandwidthGrapher(client influxdb2.Client, bwc *metrics.BandwidthCounter) {
	// TODO: using a custom bandwidth counter might make sense here...

	byproto := bwc.GetBandwidthByProtocol()
	bypeer := bwc.GetBandwidthByPeer()

	writeAPI := client.WriteAPI("estuary", "bandwidth")

	t := time.Now()
	for p, v := range byproto {
		writeAPI.WritePoint(influxdb2.NewPointWithMeasurement("stat").
			AddTag("unit", "bytespersecond").
			AddTag("protocol", string(p)).
			AddField("ratein", v.RateIn).
			AddField("rateout", v.RateOut).
			AddField("totalin", v.TotalIn).
			AddField("totalout", v.TotalOut).
			SetTime(t))
	}

	for p, v := range bypeer {
		writeAPI.WritePoint(influxdb2.NewPointWithMeasurement("stat").
			AddTag("unit", "bytespersecond").
			AddTag("peer", p.String()).
			AddField("ratein", v.RateIn).
			AddField("rateout", v.RateOut).
			AddField("totalin", v.TotalIn).
			AddField("totalout", v.TotalOut).
			SetTime(t))
	}
}
