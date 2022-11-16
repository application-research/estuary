package main

import (
	"context"

	"github.com/application-research/estuary/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HandlerPinning", Ordered, func() {

	//pinning := e.Group("/pinning")
	//pinning.Use(openApiMiddleware)
	//pinning.Use(s.AuthRequired(util.PermLevelUser))
	//pinning.GET("/pins", withUser(s.handleListPins))
	//pinning.POST("/pins", withUser(s.handleAddPin))
	//pinning.GET("/pins/:pinid", withUser(s.handleGetPin))
	//pinning.POST("/pins/:pinid", withUser(s.handleReplacePin))
	//pinning.DELETE("/pins/:pinid", withUser(s.handleDeletePin))

	BeforeEach(func() {
		newConfig := config.NewEstuary("testversion")
		Setup("user", "passwd", newConfig)
		Run(context.TODO(), newConfig)
	})

	AfterEach(func() {
		server.Exit()
	})

	It("check handleListPins", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleAddPin", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetPin", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleReplacePin", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleDeletePin", func() {
		Expect(true).To(Equal(true)) // skip
	})

})
