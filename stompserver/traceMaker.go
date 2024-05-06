package main

// tracerMaker -  It handles data that share the same data structure:
// CMSSWPOP: /topic/cms.swpop
// xrootd: /topic/xrootd.cms.aaa.ng
// Process it, then produce a Ruci trace message and then it to topic:
// /topic/cms.rucio.tracer
//
// Authors: Yuyi Guo
// Created: September 2021

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

// Define the domain and RSE map file
var domainRSEMap []DomainRSE

// TopicRecord defines the common record structure.
type TopicRecord struct {
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

// prometheus metrics
var (
	Received_swpop = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_swpop_received",
		Help: "The number of received messages of swpop",
	})
	Send_swpop = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_swpop_send",
		Help: "The number of send messages od swpop",
	})
	Traces_swpop = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_swpop_traces",
		Help: "The number of traces messages os swpop",
	})
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
var Receivedperk_2 uint64

// DSConsumer consumes from various topics, such as aaa/xrtood topic
func DSConsumer(msg *stomp.Message, topic string) (string, []string, string, string, int64, string, error) {
	//first to check to make sure there is something in msg,
	//otherwise we will get error.
	//
	atomic.AddUint64(&Receivedperk_2, 1)
	if topic == "xrtd" {
		Received_xrtd.Inc()
	} else if topic == "swpop" {
		Received_swpop.Inc()
	} else {
		return "", nil, "", "", 0, "", errors.New("topic is not supported.")
	}
	if msg == nil || msg.Body == nil {
		return "", nil, "", "", 0, "", errors.New("Empty message")
	}
	//
	if Config.Verbose > 2 {
		log.Printf("*****************Source AMQ message of %s*********************\n", topic)
		log.Println("\n" + string(msg.Body))
		log.Printf("*******************End AMQ message of %s **********************\n", topic)
	}

	var rec TopicRecord
	err := json.Unmarshal(msg.Body, &rec)
	if err != nil {
		log.Printf("Enable to Unmarchal input message. Error: %v", err)
		return "", nil, "", "", 0, "", err
	}
	if Config.Verbose > 2 {
		log.Printf(" ******Parsed %s record******\n", topic)
		log.Println("\n", rec)
		log.Printf("\n******End parsed %s record******\n", topic)
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

// TraceSender makes a trace and send it to rucio endpoint
func traceSender(msg *stomp.Message, topic string) ([]string, error) {
	var dids []string
	//get trace data
	lfn, sitename, usrdn, jobtype, ts, wnname, err := DSConsumer(msg, topic)
	if err != nil {
		log.Printf("Bad %s message.\n", topic)

		return nil, errors.New(fmt.Sprintf("Bad %s message", topic))
	}
	for _, s := range sitename {
		// gridJobErrorMessage is relevant only for fwjr/wmarchive traces
		var gridJobErrorMessage = nil
		trc := NewTrace(lfn, s, ts, jobtype, wnname, topic, usrdn, gridJobErrorMessage)
		data, err := json.Marshal(trc)
		if err != nil {
			if Config.Verbose > 1 {
				log.Printf("Unable to marshal back to JSON string , error: %v, data: %v\n", err, trc)
			} else {
				log.Printf("Unable to marshal back to JSON string, error: %v \n", err)
			}
			dids = append(dids, fmt.Sprintf("%v", trc.DID))
			continue
		}
		if Config.Verbose > 2 {
			log.Printf("********* Rucio trace record from %s ***************\n", topic)
			log.Println("\n" + string(data))
			log.Printf("\n******** Done Rucio trace record from %s *************\n", topic)
		}
		// a good trace made
		if topic == "xrtd" {
			Traces_xrtd.Inc()
		} else if topic == "swpop" {
			Traces_swpop.Inc()
		} else {
			log.Fatalf(" Topic %s is not supported. \n", topic)
		}
		// send data to Stomp endpoint
		if Config.EndpointProducer != "" {
			err := stompMgr.Send(data, stomp.SendOpt.Header("appversion", "xrootdAMQ"))
			if err != nil {
				dids = append(dids, fmt.Sprintf("%v", trc.DID))
				log.Printf("Failed to send %s to stomp.", trc.DID)
			} else {
				if topic == "xrtd" {
					Send_xrtd.Inc()
				} else if topic == "swpop" {
					Send_swpop.Inc()
				} else {
					return nil, errors.New("topic is not supported.")
				}

			}
		} else {
			log.Fatalln("*** Config.Enpoint is empty, check config file! ***")
		}
	}
	return dids, nil
}

//
// TraceServer gets messages from consumer AMQ end pointer, make tracers and send to AMQ producer end point.
func traceServer(topic string) {
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
	var dids []string
	var err error
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
			if topic == "fwjr" {
				dids, err = FWJRtrace(msg)
			} else {
				dids, err = traceSender(msg, topic)
			}
			if err == nil {
				atomic.AddUint64(&tc, 1)
				if Config.Verbose > 1 {
					log.Println("The number of messages processed in 1000 group: ", atomic.LoadUint64(&tc))
				}
			}

			if atomic.LoadUint64(&tc) == 1000 {
				atomic.StoreUint64(&tc, 0)
				t2 = time.Now().Unix() - t1
				t1 = time.Now().Unix()
				if topic == "fwjr" {
					log.Printf("Processing 1000 %s messages while total received %d messages.\n", topic, atomic.LoadUint64(&Receivedperk))
					atomic.StoreUint64(&Receivedperk, 0)
				} else {
					log.Printf("Processing 1000 %s messages while total received %d messages.\n", topic, atomic.LoadUint64(&Receivedperk_2))
					atomic.StoreUint64(&Receivedperk_2, 0)
				}
				log.Printf("Processing 1000 messages took %d seconds.\n", t2)
			}
			if err != nil && err.Error() != "Empty message" {
				log.Printf("\n %s message processing error: %v\n", topic, err)
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
				if Config.Verbose > 0 {
					log.Println("waiting for 10000x", sleep)
				}
			}
			time.Sleep(sleep)
			atomic.AddUint64(&ts, 1)
		}
	}
}
