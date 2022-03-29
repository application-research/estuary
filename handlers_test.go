package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	cli "github.com/urfave/cli/v2"
)

var app *cli.App

func setupTestCase(t *testing.T) func(t *testing.T) {
	app = GetApp()
	os.Args = []string{"./estuary", "--datadir=."}
	go app.Run(os.Args) // TODO: add some actual args

	t.Log("setup test case")
	return func(t *testing.T) {
		t.Log("teardown test case")
	}
}

func TestAutoretrieveInit(t *testing.T) {
	cases := []struct {
		name           string
		payloadAddrs   string
		payloadPrivKey string
		expectedAddrs  string
		expectedID     string
	}{
		{
			name:           "regular",
			payloadAddrs:   "/ip4/192.168.15.7/tcp/6744/p2p/12D3KooWPmL4jXD2y1sgzyyYc36XUoNw1rmLnsg5woPF24vabEVR",
			payloadPrivKey: "CAESQAcAmGX1A0sBeSi53lqzuLncZ5vYvnJFe4gQOFB/719e5PuDbjPiDAOfLTNPWvZXny19mXnOZnXf1RXF3fKqdP8=",
			expectedAddrs:  `["/ip4/192.168.15.7/tcp/6744"]`,
			expectedID:     "12D3KooWPmL4jXD2y1sgzyyYc36XUoNw1rmLnsg5woPF24vabEVR",
		},
	}

	teardownTestCase := setupTestCase(t)
	defer teardownTestCase(t)

	apiKey := "EST191c8316-0960-489c-a49c-c8904ba13a05ARY"
	// e := echo.New()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			reqJSON := fmt.Sprintf(`{"privateKey": "%s", "addresses": "%s"}`, tc.payloadPrivKey, tc.payloadAddrs)
			req := httptest.NewRequest(http.MethodPost, "/admin/autoretrieve/init", strings.NewReader(reqJSON))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			req.Header.Set("Authorization", "Bearer "+apiKey)

			rec := httptest.NewRecorder()
			t.Logf("%+v\n", rec.Result())
			// c := e.NewContext(req, rec)
			// if assert.NoError(t, app.s.createUser(c)) {
			// 	assert.Equal(t, http.StatusCreated, rec.Code)
			// 	assert.Equal(t, reqJSON, rec.Body.String())
			// }

		})
	}
}

// 	// Setup
// 	h := &Server{mockDB}

// 	// Assertions
// }
