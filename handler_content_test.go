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
	content_add_car             = base_url + "/content/add-car"
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
		body, writer, _ := getMockBody()
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
		body, writer, _ := getMockBody()
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
		body, writer, _ := getMockBody()
		req := httptest.NewRequest(echo.POST, content_add_car, body)
		req.Header.Set(echo.HeaderContentType, writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleAddIpfs(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
		Expect(true).To(Equal(true)) // skip
	})

	It("check handleCreateContent", func() {
		body, writer, _ := getMockBody()
		req := httptest.NewRequest(echo.POST, content_add_url, body)
		req.Header.Set(echo.HeaderContentType, writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleCreateContent(ctx, &User{
			Model: gorm.Model{ID: 1},
		})

	})

	It("check handleGetContentByCid", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		req.Form.Set("cid", "")
		s.handleGetContentByCid(ctx)
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleStats", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleStats(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleEnsureReplication", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		req.Form.Set("datacid", "")
		s.handleEnsureReplication(ctx)
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleContentStatus", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		req.Form.Set("id", "")
		s.handleContentStatus(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleListContent", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleListContent(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleListContentWithDeals", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleListContentWithDeals(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleGetContentFailures", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleGetContentFailures(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleGetContentBandwidth", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleGetContentBandwidth(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleGetStagingZoneForUser", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleGetStagingZoneForUser(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})
	It("check handleGetAggregatedForContent", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleGetAggregatedForContent(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

	It("check handleGetAllDealsForUser", func() {
		req := httptest.NewRequest(echo.GET, content_add_url, nil)
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(req, rec)
		s.handleGetAllDealsForUser(ctx, &User{
			Model: gorm.Model{ID: 1},
		})
		fmt.Println(rec.Body.String())
		Expect(rec.Code).To(Equal(200))
	})

})

func getMockBody() (*bytes.Buffer, *multipart.Writer, error) {
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	file, _ := os.Open(filePath)
	w, _ := writer.CreateFormFile("data", filePath)

	_, _ = io.Copy(w, file)
	writer.Close()

	return body, writer, nil
}
