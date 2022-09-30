package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("HandlerDeals", Ordered, func() {

	It("run the handler tests for deals", func() {
		//time.Tick(time.Second * 5)
		time.Sleep(time.Second * 5)
	})

	//deals := e.Group("/deals")
	//deals.Use(s.AuthRequired(util.PermLevelUser))
	//deals.GET("/status/:deal", withUser(s.handleGetDealStatus))
	//deals.GET("/status-by-proposal/:propcid", withUser(s.handleGetDealStatusByPropCid))
	//deals.GET("/query/:miner", s.handleQueryAsk)
	//deals.POST("/make/:miner", withUser(s.handleMakeDeal))
	////deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	//deals.GET("/transfer/status/:id", s.handleTransferStatusByID)
	//deals.POST("/transfer/status", s.handleTransferStatus)
	//deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	//deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	//deals.POST("/estimate", s.handleEstimateDealCost)
	//deals.GET("/proposal/:propcid", s.handleGetProposal)
	//deals.GET("/info/:dealid", s.handleGetDealInfo)
	//deals.GET("/failures", withUser(s.handleStorageFailures))

	It("check handleGetDealStatus", func() {

		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetDealStatusByPropCid", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleQueryAsk", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleMakeDeal", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleTransferStart", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleTransferStatusByID", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleTransferStatus", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleTransferInProgress", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleDealStatus", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleEstimateDealCost", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetProposal", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetDealInfo", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleStorageFailures", func() {
		Expect(true).To(Equal(true)) // skip
	})
})
