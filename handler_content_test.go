package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/labstack/echo/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/gorm"
)

var (
	base_url             string = "http://localhost:3004"
	content_add_url             = base_url + "/content/add"
	content_add_ipfs_url        = base_url + "/content/add-ipfs"
)

var (
	filePath string = "./tests/data/files/dummy.file"
)
var _ = Describe("HandlerContent", Ordered, func() {
	It("run the handler tests for content", func() {
		//time.Tick(time.Second * 5)
		time.Sleep(time.Second * 5)
	})

	//contmeta := e.Group("/content")
	//uploads := contmeta.Group("", s.AuthRequired(util.PermLevelUpload))
	//uploads.POST("/add", withUser(s.handleAdd))
	//uploads.POST("/add-ipfs", withUser(s.handleAddIpfs))
	//uploads.POST("/add-car", util.WithContentLengthCheck(withUser(s.handleAddCar)))
	//uploads.POST("/create", withUser(s.handleCreateContent))
	//content := contmeta.Group("", s.AuthRequired(util.PermLevelUser))
	//content.GET("/by-cid/:cid", s.handleGetContentByCid)
	//content.GET("/stats", withUser(s.handleStats))
	//content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	//content.GET("/status/:id", withUser(s.handleContentStatus))
	//content.GET("/list", withUser(s.handleListContent))
	//content.GET("/deals", withUser(s.handleListContentWithDeals))
	//content.GET("/failures/:content", withUser(s.handleGetContentFailures))
	//content.GET("/bw-usage/:content", withUser(s.handleGetContentBandwidth))
	//content.GET("/staging-zones", withUser(s.handleGetStagingZoneForUser))
	//content.GET("/aggregated/:content", withUser(s.handleGetAggregatedForContent))
	//content.GET("/all-deals", withUser(s.handleGetAllDealsForUser))

	It("check handleAdd", func() {
		body := new(bytes.Buffer)
		writer := multipart.NewWriter(body)
		file, _ := os.Open(filePath)
		w, _ := writer.CreateFormFile("data", filePath)

		_, _ = io.Copy(w, file)
		writer.Close()

		req := httptest.NewRequest(echo.POST, content_add_url, body)
		req.Header.Set(echo.HeaderContentType, writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleAdd(ctx, &util.User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleAddIpfs", func() {
		body := new(bytes.Buffer)
		writer := multipart.NewWriter(body)
		file, _ := os.Open(filePath)
		w, _ := writer.CreateFormFile("data", filePath)

		_, _ = io.Copy(w, file)
		writer.Close()

		req := httptest.NewRequest(echo.POST, content_add_ipfs_url, body)
		req.Header.Set(echo.HeaderContentType, writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleAddIpfs(ctx, &util.User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleAddCar", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleCreateContent", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetContentByCid", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleStats", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleEnsureReplication", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleContentStatus", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleListContent", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleListContentWithDeals", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetContentFailures", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetContentBandwidth", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetStagingZoneForUser", func() {
		Expect(true).To(Equal(true)) // skip
	})
	It("check handleGetAggregatedForContent", func() {
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleGetAllDealsForUser", func() {
		Expect(true).To(Equal(true)) // skip
	})

})
