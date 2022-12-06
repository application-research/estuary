package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	pgd "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	app := getApp()

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// App constructor (so we can test against it)
func getApp() *cli.App {
	app := cli.NewApp()

	app.Name = "benchest"
	app.Commands = []*cli.Command{
		benchAddFileCmd,
		benchFetchFileCmd,
		benchAddResultCmd,
	}

	return app
}

func getFile(cctx *cli.Context) (io.Reader, string, error) {
	buf := make([]byte, 1024*1024)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, "", err
	}

	return bytes.NewReader(buf), fmt.Sprintf("goodfile-%x", buf[:4]), nil
}

type benchResult struct {
	Runner          string
	BenchStart      time.Time
	FileCID         string
	AddFileRespTime time.Duration
	AddFileTime     time.Duration
	AddFileError    string

	FetchStats *fetchStats
	IpfsCheck  *checkResp
}

var benchAddFileCmd = &cli.Command{
	Name: "add-file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Value: "api.estuary.tech",
		},
		&cli.StringFlag{
			Name:  "runner",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "postgres",
			Value: "",
		},
		&cli.DurationFlag{
			Name:  "every",
			Usage: "run benchmark in a loop on the specified interval",
		},
	},
	Action: func(cctx *cli.Context) error {
		estToken := os.Getenv("ESTUARY_TOKEN")
		if estToken == "" {
			return fmt.Errorf("no estuary token found")
		}

		host := cctx.String("host")
		interval := cctx.Duration("every")
		runner := cctx.String("runner")

		var resdb *gorm.DB
		if pg := cctx.String("postgres"); pg != "" {
			db, err := openDB(pg)
			if err != nil {
				return err
			}
			resdb = db
		}

		for {
			start := time.Now()
			fi, name, err := getFile(cctx)
			if err != nil {
				return err
			}

			outstats, err := RunBenchAddFile(name, fi, host, estToken)
			if err != nil {
				fmt.Fprintln(os.Stderr, "failed to run bench: ", err)
				time.Sleep(time.Second * 15)
				continue
			}

			outstats.Runner = runner

			b, err := json.MarshalIndent(outstats, "", "  ")
			if err != nil {
				return err
			} else {
				fmt.Println(string(b))
			}

			if resdb != nil {
				if err := addResultsToDatabase(resdb, outstats); err != nil {
					return err
				}
			}

			if interval == 0 {
				return nil
			}
			took := time.Since(start)
			if took < interval {
				time.Sleep(interval - took)
			}
		}
	},
}

var benchAddResultCmd = &cli.Command{
	Name: "add-result",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "postgres",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) (err error) {
		if !cctx.Args().Present() {
			return fmt.Errorf("must pass filename")
		}

		db, err := openDB(cctx.String("postgres"))
		if err != nil {
			return fmt.Errorf("failed to open db: %w", err)
		}

		fi, err := os.Open(cctx.Args().First())
		if err != nil {
			return err
		}
		defer func() {
			if errC := fi.Close(); errC != nil {
				err = errC
			}
		}()

		dec := json.NewDecoder(fi)
		for {
			var res benchResult
			if err := dec.Decode(&res); err != nil {
				return fmt.Errorf("json decode: %w", err)
			}

			if err := addResultsToDatabase(db, &res); err != nil {
				return err
			}
		}
	},
}

var benchFetchFileCmd = &cli.Command{
	Name: "fetch-file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Value: "api.estuary.tech",
		},
		&cli.StringFlag{
			Name:  "file",
			Value: "QmducxoYHKULWXeq5wtKoeMzie2QggYphNCVwuFuou9eWE",
			Usage: "UploadTypeCID for file - defaults to NYC Public Data: QmducxoYHKULWXeq5wtKoeMzie2QggYphNCVwuFuou9eWE",
		},
		&cli.StringFlag{
			Name:  "runner",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "postgres",
			Value: "",
		},
		&cli.DurationFlag{
			Name:  "every",
			Usage: "run benchmark in a loop on the specified interval",
		},
	},
	Action: func(cctx *cli.Context) error {
		estToken := os.Getenv("ESTUARY_TOKEN")
		if estToken == "" {
			return fmt.Errorf("no estuary token found")
		}

		host := cctx.String("host")
		interval := cctx.Duration("every")
		runner := cctx.String("runner")
		cid := cctx.String("file")

		var resdb *gorm.DB
		if pg := cctx.String("postgres"); pg != "" {
			db, err := openDB(pg)
			if err != nil {
				return err
			}
			resdb = db
		}

		for {
			start := time.Now()

			outstats, err := RunBenchFetchFile(cid, host, estToken)
			if err != nil {
				fmt.Fprintln(os.Stderr, "failed to run bench: ", err)
				time.Sleep(time.Second * 15)
				continue
			}

			outstats.Runner = runner

			b, err := json.MarshalIndent(outstats, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))

			if resdb != nil {
				if err := addResultsToDatabase(resdb, outstats); err != nil {
					return err
				}
			}

			if interval == 0 {
				return nil
			}
			took := time.Since(start)
			if took < interval {
				time.Sleep(interval - took)
			}
		}

		return nil
	},
}

func RunBenchAddFile(name string, fi io.Reader, host string, estToken string) (*benchResult, error) {
	buf := new(bytes.Buffer)
	mw := multipart.NewWriter(buf)
	part, err := mw.CreateFormFile("data", name)
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(part, fi); err != nil {
		return nil, err
	}
	err = mw.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/content/add", host), buf)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+estToken)

	// Start of HTTP request for a file
	addReqStart := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// End of HTTP request for a file
	addRespAt := time.Now()

	if resp.StatusCode != 200 {
		var m map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			fmt.Println(err)
		}
		fmt.Fprintln(os.Stderr, "error body: ", m)
		return &benchResult{
			AddFileError: fmt.Sprintf("got invalid status code: %d", resp.StatusCode),
		}, nil
	}

	var rbody util.ContentAddResponse
	if err := json.NewDecoder(resp.Body).Decode(&rbody); err != nil {
		return nil, err
	}
	readBodyTime := time.Now()

	fmt.Fprintln(os.Stderr, "file added, cid: ", rbody.Cid)

	chk := make(chan *checkResp)
	go func() {
		if len(rbody.Providers) == 0 {
			chk <- &checkResp{
				CheckRequestError: "no addresses back from add response",
			}
			return
		}

		addr := rbody.Providers[0]
		for _, a := range rbody.Providers {
			if !strings.Contains(a, "127.0.0.1") {
				addr = a
			}
		}

		chk <- ipfsCheck(rbody.Cid, addr)
	}()

	st, err := benchFetch(rbody.Cid)
	if err != nil {
		return nil, err
	}

	chkresp := <-chk

	return &benchResult{
		BenchStart:      addReqStart,
		FileCID:         rbody.Cid,
		AddFileRespTime: addRespAt.Sub(addReqStart),
		AddFileTime:     readBodyTime.Sub(addReqStart),

		FetchStats: st,
		IpfsCheck:  chkresp,
	}, nil
}

func RunBenchFetchFile(cid string, host string, estToken string) (*benchResult, error) {
	// Start of HTTP request for a file
	addReqStart := time.Now()

	st, err := benchFetch(cid)
	if err != nil {
		return nil, err
	}

	// End of HTTP request for a file
	addRespAt := time.Now()

	return &benchResult{
		BenchStart:      addReqStart,
		FileCID:         cid,
		AddFileRespTime: addRespAt.Sub(addReqStart),
		AddFileTime:     addRespAt.Sub(addReqStart),

		FetchStats: st,
		IpfsCheck:  nil,
	}, nil
}

type fetchStats struct {
	RequestStart time.Time
	GatewayURL   string

	GatewayHost  string
	StatusCode   int
	RequestError string

	ResponseTime      time.Duration
	TimeToFirstByte   time.Duration
	TotalTransferTime time.Duration
	TotalElapsed      time.Duration
}

func benchFetch(c string) (*fetchStats, error) {
	url := "https://dweb.link/ipfs/" + c
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	afterDo := time.Now()
	if err != nil {
		return &fetchStats{
			RequestStart: start,
			TotalElapsed: time.Since(start),
			RequestError: err.Error(),
		}, nil
	}

	gwayhost := resp.Header.Get("x-ipfs-gateway-host")
	xpop := resp.Header.Get("x-ipfs-pop")
	if gwayhost == "" {
		gwayhost = xpop
	}

	status := resp.StatusCode

	defer resp.Body.Close()

	br := bufio.NewReader(resp.Body)

	_, err = br.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("peeking first byte failed: %w", err)
	}
	firstByteAt := time.Now()

	_, err = io.Copy(io.Discard, br)
	if err != nil {
		return nil, fmt.Errorf("copying bytes failed: %w ", err)
	}
	endTime := time.Now()

	return &fetchStats{
		RequestStart: start,
		GatewayURL:   url,
		StatusCode:   status,

		GatewayHost: gwayhost,

		ResponseTime:      afterDo.Sub(start),
		TimeToFirstByte:   firstByteAt.Sub(start),
		TotalTransferTime: endTime.Sub(firstByteAt),
		TotalElapsed:      endTime.Sub(start),
	}, nil
}

type checkResp struct {
	CheckTook                time.Duration
	CheckRequestError        string
	ConnectionError          string
	PeerFoundInDHT           map[string]int
	CidInDHT                 bool
	DataAvailableOverBitswap struct {
		Found     bool
		Responded bool
		Error     string
	}
}

func ipfsCheck(c string, maddr string) *checkResp {
	start := time.Now()
	resp, err := http.DefaultClient.Get(fmt.Sprintf("https://ipfs-check-backend.ipfs.io/?cid=%s&multiaddr=%s", c, maddr))
	if err != nil {
		return &checkResp{
			CheckTook:         time.Since(start),
			CheckRequestError: err.Error(),
		}
	}

	defer resp.Body.Close()

	var out checkResp
	out.CheckTook = time.Since(start)
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return &checkResp{
			CheckTook:         time.Since(start),
			CheckRequestError: err.Error(),
		}
	}

	return &out
}

type DBBenchResult struct {
	ID     uint
	Result pgd.Jsonb
}

func openDB(dbstr string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(dbstr), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	/*
		if err := db.AutoMigrate(&DBBenchResult{}); err != nil {
			return nil, err
		}
	*/

	return db, nil
}

func addResultsToDatabase(db *gorm.DB, data *benchResult) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return db.Create(&DBBenchResult{
		Result: pgd.Jsonb{RawMessage: json.RawMessage(b)},
	}).Error
}
