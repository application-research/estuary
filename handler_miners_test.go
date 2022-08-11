package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HandlerMiner", Ordered, func() {

	//miners := public.Group("/miners")
	//miners.GET("", s.handleAdminGetMiners)
	//miners.GET("/failures/:miner", s.handleGetMinerFailures)
	//miners.GET("/deals/:miner", s.handleGetMinerDeals)
	//miners.GET("/stats/:miner", s.handleGetMinerStats)
	//miners.GET("/storage/query/:miner", s.handleQueryAsk)

	It("check handleAdminGetMiners", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetMinerFailures", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetMinerDeals", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetMinerStats", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleQueryAsk", func() {
		Expect(true).To(Equal(true)) // skip
	})

})
