package main

// xrootdTracer - Is one of the three RucioTracer. It handles data from
// xrootd: /topic/xrootd.cms.aaa.ng
// Process it, then produce a Ruci trace message and then it to topic:
// /topic/cms.rucio.tracer
//
// Authors: Yuyi Guo
// Created: July 2021

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	// stomp library
	"github.com/go-stomp/stomp"

	// prometheus apis
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// prometheus metrics
var (
	Received_xrtd = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_xrootd_received",
		Help: "The number of received messages",
	})
	Send_xrtd = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_xrootd_send",
		Help: "The number of send messages",
	})
	Traces_xrtd = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_xrootd_traces",
		Help: "The number of traces messages",
	})
)

// SWPOPRecord defines CMSSW POP record structure.
type XrtdRecord struct {
	SiteName     string `json:"site_name"`
	Usrdn        string `json:"user_dn"`
	ClientHost   string `json:"client_host"`
	ClientDomain string `json:"client_domain"`
	ServerHost   string `json:"server_host"`
	ServerDomain string `json:"server_domain"`
	ServerSite   string `json:"server_site"`
	Lfn          string `json:"file_lfn"`
	JobType      string `json:"app_info"`
	Ts           int64  `json:"start_time"`
}

// Define the domain and RSE map file
var domainRSEMap2 []DomainRSE

// Receivedperk keeps number of messages per 1k
var Receivedperk_xrtd uint64

// xrtdConsumer consumes for aaa/xrtood topic
func xrtdConsumer(msg *stomp.Message) (string, []string, string, string, int64, string, error) {
	//first to check to make sure there is something in msg,
	//otherwise we will get error.
	//
	Received_xrtd.Inc()
	atomic.AddUint64(&Receivedperk_xrtd, 1)
	if msg == nil || msg.Body == nil {
		return "", nil, "", "", 0, "", errors.New("Empty message")
	}
	//
	if Config.Verbose > 2 {
		log.Println("*****************Source AMQ message of xrtd*********************")
		log.Println("\n" + string(msg.Body))
		log.Println("*******************End AMQ message of xrtd**********************")
	}

	var rec XrtdRecord
	err := json.Unmarshal(msg.Body, &rec)
	if err != nil {
		log.Printf("Enable to Unmarchal input message. Error: %v", err)
		return "", nil, "", "", 0, "", err
	}
	if Config.Verbose > 2 {
		log.Println(" ******Parsed xrootd record******")
		log.Println("\n", rec)
		log.Println("******End parsed xrootd record******")
	}
	// process received message, e.g. extract some fields
	var lfn string
	var sitename []string
	var usrdn string
	var ts int64
	var jobtype string
	var wnname string
	var site string
	// Check the data
	if len(rec.Lfn) > 0 {
		lfn = rec.Lfn
	} else {
		return "", nil, "", "", 0, "", errors.New("No Lfn found")
	}
	if strings.ToLower(rec.ServerSite) != "unknown" || len(rec.ServerSite) > 0 {
		if s, ok := Sitemap[rec.ServerSite]; ok {
			site = s
		} else {
			site = rec.ServerSite
		}
		// one lfn may have more than one RSE in some cases, such as in2p3.fr
		sitename = append(sitename, site)
	} else if strings.ToLower(rec.ServerDomain) == "unknown" || len(rec.ServerDomain) <= 0 {
		if len(rec.SiteName) == 0 {
			return "", nil, "", "", 0, "", errors.New("No RSEs found")
		} else {
			if s, ok := Sitemap[rec.SiteName]; ok {
				site = s
			} else {
				site = rec.SiteName
			}
			// one lfn may have more than one RSE in some cases, such as in2p3.fr
			sitename = append(sitename, site)
		}
	} else {
		sitename = findRSEs(rec.ServerDomain)
	}
	if len(sitename) <= 0 {
		return "", nil, "", "", 0, "", errors.New("No RSEs' map found")
	}
	//
	if rec.Ts == 0 {
		ts = time.Now().Unix()
	} else {
		ts = rec.Ts
	}
	//

	if len(rec.JobType) > 0 {
		jobtype = rec.JobType
	} else {
		jobtype = "unknow"
	}
	//
	if len(rec.ClientDomain) > 0 {
		wnname = rec.ClientDomain
	} else {
		wnname = "unknow"
	}
	if len(rec.ClientHost) > 0 {
		wnname = rec.ClientHost + "@" + wnname
	} else {
		wnname = "unknow" + "@" + wnname
	}
	//
	if len(rec.Usrdn) > 0 {
		usrdn = rec.Usrdn
	} else {
		usrdn = ""
	}
	return lfn, sitename, usrdn, jobtype, ts, wnname, nil
}

// xrtdTrace makes xrootd/aaa trace and send it to rucio endpoint
func xrtdTrace(msg *stomp.Message) ([]string, error) {
	var dids []string
	//get trace data
	lfn, sitename, usrdn, jobtype, ts, wnname, err := xrtdConsumer(msg)
	if err != nil {
		log.Println("Bad xroot message.")
		return nil, errors.New("Bad xrootd message")
	}
	for _, s := range sitename {
		trc := NewTrace(lfn, s, ts, jobtype, wnname, "xrootd", usrdn)
		data, err := json.Marshal(trc)
		if err != nil {
			if Config.Verbose > 1 {
				log.Printf("Unable to marshal back to JSON string , error: %v, data: %v\n", err, trc)
			} else {
				log.Printf("Unable to marshal back to JSON string, error: %v \n", err)
			}
			dids = append(dids, fmt.Sprintf("%v", trc.DID))
		}
		if Config.Verbose > 2 {
			log.Println("********* Rucio trace record ***************")
			log.Println("\n" + string(data))
			log.Println("******** Done Rucio trace record *************")
		}
		// send data to Stomp endpoint
		if Config.EndpointProducer != "" {
			err := stompMgr.Send(data, stomp.SendOpt.Header("appversion", "xrootdAMQ"))
			if err != nil {
				dids = append(dids, fmt.Sprintf("%v", trc.DID))
				log.Printf("Failed to send %s to stomp.", trc.DID)
			} else {
				Send_xrtd.Inc()
			}
		} else {
			log.Fatalln("*** Config.Enpoint is empty, check config file! ***")
		}
	}
	return dids, nil
}

//
// xrtdServer gets messages from consumer AMQ end pointer, make tracers and send to AMQ producer end point.
func xrtdServer() {
	log.Println("Consumer Stomp broker URL: ", Config.StompURIConsumer)
	//
	err2 := parseRSEMap(fdomainmap)
	if err2 != nil {
		log.Fatalf("Unable to parse rucio doamin RSE map file %s, error: %v \n", fdomainmap, err2)
	}
	//
	err2 = parseSitemap(fsitemap)
	if err2 != nil {
		log.Fatalf("Unable to parse rucio sitemap file %s, error: %v \n", fsitemap, err2)
	}
	//
	var tc uint64
	t1 := time.Now().Unix()
	var t2 int64
	var ts uint64
	var restartSrv uint
	smgr := initStomp(Config.EndpointConsumer, Config.StompURIConsumer)
	// ch for all the listeners to write to
	ch := make(chan *stomp.Message)
	// defer close executed when the main function is about to exit.
	// In this way the channel is to be closed and no resources taken.
	defer close(ch)
	for _, addr := range smgr.Addresses {
		go listener(smgr, addr, ch)
	}
	//
	for {
		// get stomp messages from ch
		select {
		case msg := <-ch:
			restartSrv = 0
			if msg.Err != nil {
				break
			}
			// process stomp messages
			dids, err := xrtdTrace(msg)
			if err == nil {
				Traces_xrtd.Inc()
				atomic.AddUint64(&tc, 1)
				if Config.Verbose > 1 {
					log.Println("The number of traces processed in 1000 group: ", atomic.LoadUint64(&tc))
				}
			}

			if atomic.LoadUint64(&tc) == 1000 {
				atomic.StoreUint64(&tc, 0)
				t2 = time.Now().Unix() - t1
				t1 = time.Now().Unix()
				log.Printf("Processing 1000 messages while total received %d messages.\n", atomic.LoadUint64(&Receivedperk_xrtd))
				log.Printf("Processing 1000 messages took %d seconds.\n", t2)
				atomic.StoreUint64(&Receivedperk_xrtd, 0)
			}
			if err != nil && err.Error() != "Empty message" {
				log.Println("xrootd/aaa message processing error", err)
			}
			if len(dids) > 0 {
				log.Printf("DIDS in Error: %v .\n ", dids)
			}
		default:
			sleep := time.Duration(Config.Interval) * time.Millisecond
			// Config.Interval = 1 so each sleeping is 10 ms. We will have to restart the server
			// if it cannot get any messages in 5 minutes.
			if restartSrv >= 300000 {
				//FIXME: We may not exit anymore.
				log.Fatalln("No messages in 5 minutes, exit(1)")
			}
			restartSrv += 1
			if atomic.LoadUint64(&ts) == 10000 {
				atomic.StoreUint64(&ts, 0)
				if Config.Verbose > 3 {
					log.Println("waiting for 10000x", sleep)
				}
			}
			time.Sleep(sleep)
			atomic.AddUint64(&ts, 1)
		}
	}
}
