package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/application-research/estuary/util"
)

type ShuttleHttpClient struct {
	estuaryHost string
	dev         bool
}

func NewShuttleHttpClient(estuaryHost string, dev bool) *ShuttleHttpClient {
	shc := ShuttleHttpClient{
		estuaryHost: estuaryHost,
		dev:         dev,
	}

	return &shc
}

// Makes an HTTP to the main Estuary API with given parameters
func (shc *ShuttleHttpClient) MakeRequest(method string, url string, body io.Reader, authToken string) (*http.Response, error) {
	scheme := "https"
	if shc.dev {
		scheme = "http"
	}

	req, err := http.NewRequest(method, scheme+"://"+shc.estuaryHost+url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var out util.HttpErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, err
		}
		return nil, &out.Error
	}

	return resp, nil
}
