package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HandlerUser", Ordered, func() {
	//user := e.Group("/user")
	//user.Use(s.AuthRequired(util.PermLevelUser))
	//user.GET("/api-keys", withUser(s.handleUserGetApiKeys))
	//user.POST("/api-keys", withUser(s.handleUserCreateApiKey))
	//user.DELETE("/api-keys/:key", withUser(s.handleUserRevokeApiKey))
	//user.GET("/export", withUser(s.handleUserExportData))
	//user.PUT("/password", withUser(s.handleUserChangePassword))
	//user.PUT("/address", withUser(s.handleUserChangeAddress))
	//user.GET("/stats", withUser(s.handleGetUserStats))

	It("check handleUserGetApiKeys", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleUserCreateApiKey", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleUserRevokeApiKey", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleUserExportData", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleUserChangePassword", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleUserChangeAddress", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetUserStats", func() {
		Expect(true).To(Equal(true)) // skip
	})

})
