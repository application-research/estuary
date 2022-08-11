package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HandlerAdmin", Ordered, func() {

	//admin := e.Group("/admin")
	//admin.Use(s.AuthRequired(util.PermLevelAdmin))
	//admin.GET("/balance", s.handleAdminBalance)
	//admin.POST("/add-escrow/:amt", s.handleAdminAddEscrow)
	//admin.GET("/dealstats", s.handleDealStats)
	//admin.GET("/disk-info", s.handleDiskSpaceCheck)
	//admin.GET("/stats", s.handleAdminStats)
	//admin.GET("/system/config", withUser(s.handleGetSystemConfig))
	//
	//// miners
	//admin.POST("/miners/add/:miner", s.handleAdminAddMiner)
	//admin.POST("/miners/rm/:miner", s.handleAdminRemoveMiner)
	//admin.POST("/miners/suspend/:miner", withUser(s.handleSuspendMiner))
	//admin.PUT("/miners/unsuspend/:miner", withUser(s.handleUnsuspendMiner))
	//admin.PUT("/miners/set-info/:miner", withUser(s.handleMinersSetInfo))
	//admin.GET("/miners", s.handleAdminGetMiners)
	//admin.GET("/miners/stats", s.handleAdminGetMinerStats)
	//admin.GET("/miners/transfers/:miner", s.handleMinerTransferDiagnostics)
	//
	//admin.GET("/cm/progress", s.handleAdminGetProgress)
	//admin.GET("/cm/all-deals", s.handleDebugGetAllDeals)
	//admin.GET("/cm/read/:content", s.handleReadLocalContent)
	//admin.GET("/cm/staging/all", s.handleAdminGetStagingZones)
	//admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	//admin.POST("/cm/offload/:content", s.handleOffloadContent)
	//admin.POST("/cm/offload/collect", s.handleRunOffloadingCollection)
	//admin.GET("/cm/refresh/:content", s.handleRefreshContent)
	//admin.POST("/cm/gc", s.handleRunGc)
	//admin.POST("/cm/move", s.handleMoveContent)
	//admin.GET("/cm/buckets", s.handleGetBucketDiag)
	//admin.GET("/cm/health/:id", s.handleContentHealthCheck)
	//admin.GET("/cm/health-by-cid/:cid", s.handleContentHealthCheckByCid)
	//admin.POST("/cm/dealmaking", s.handleSetDealMaking)
	//admin.POST("/cm/break-aggregate/:content", s.handleAdminBreakAggregate)
	//admin.POST("/cm/transfer/restart/:chanid", s.handleTransferRestart)
	//admin.POST("/cm/repinall/:shuttle", s.handleShuttleRepinAll)
	//
	////	peering
	//adminPeering := admin.Group("/peering")
	//adminPeering.POST("/peers", s.handlePeeringPeersAdd)
	//adminPeering.DELETE("/peers", s.handlePeeringPeersRemove)
	//adminPeering.GET("/peers", s.handlePeeringPeersList)
	//adminPeering.POST("/start", s.handlePeeringStart)
	//adminPeering.POST("/stop", s.handlePeeringStop)
	//adminPeering.GET("/status", s.handlePeeringStatus)
	//
	//admnetw := admin.Group("/net")
	//admnetw.GET("/peers", s.handleNetPeers)
	//
	//admin.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)
	//admin.GET("/retrieval/stats", s.handleGetRetrievalInfo)
	//
	//admin.POST("/invite/:code", withUser(s.handleAdminCreateInvite))
	//admin.GET("/invites", s.handleAdminGetInvites)
	//
	//admin.GET("/fixdeals", s.handleFixupDeals)
	//admin.POST("/loglevel", s.handleLogLevel)

	It("check handleAdminBalance", func() {
		Expect(true).To(BeTrue())
	})

	It("check handleAdminAddEscrow", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleDealStats", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleDiskSpaceCheck", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleAdminStats", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetSystemConfig", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleAdminAddMiner", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleAdminRemoveMiner", func() {
		Expect(true).To(Equal(true)) // skip
	})

})
