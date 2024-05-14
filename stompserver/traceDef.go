package main

// trace_def defines CMS trace structure and how to create a new trace.
//
// Authors: Yuyi Guo
// Created: June 2021

import "fmt"

//Trace defines Rucio trace
type Trace struct {
	// EventVersion  default value API_1.21.6
	EventVersion string `json:"eventVersion"`
	// ClientState default value done
	ClientState string `json:"clientState"`
	// Scope  default value cms
	Scope string `json:"scope"`
	// EventType  default value get
	EventType string `json:"eventType"`
	// Usrdn default value /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fwjr/CN=1/CN=fwjr/CN=0
	Usrdn string `json:"usrdn"`
	// Account default fwjr, other options are crab, cmspop, xrootd and so on.
	Account string `json:"account"`
	// Filename defines cms LFN.
	Filename string `json:"filename"`
	// RemoteSite defines where the file was read from.
	RemoteSite string `json:"remoteSite"`
	// DID is defined as cms:lfn
	DID string `json:"DID"`
	// FileReadts defines when the file is read.
	FileReadts int64 `json:"file_read_ts"`
	// Jobtype defines the type of job.
	Jobtype string `json:"jobtype"`
	// Wnname defines the name of worknode.
	Wnname string `json:"wn_name"`
	// Timestamp defines the file read timestamp, same as FileReadts.
	Timestamp int64 `json:"timestamp"`
	// TraceTimeentryUnix defines when the trace was enteried, same as FileReadts.
	TraceTimeentryUnix int64 `json:"traceTimeentryUnix"`
	// StateReason stores the error message if any
	StateReason string `json:"stateReason"`
}

// NewTrace creates a new instance of Rucio Trace.
func NewTrace(lfn string, site string, ts int64, jobtype string, wnname string, account string, usrdn string, gridJobErrorMessage string, clientState string) Trace {
	if account == "" {
		account = "fwjr"
	}
	if usrdn == "" {
		usrdn = "unknown"
	}
	if clientState == "" {
		clientState = "DONE"
	}
	// some of the ts is using milliseconds , such as xrootd data. We fix here to avoid going to each data source
	if ts > 1000000000000 {
		ts = ts / 1000
	}
	trc := Trace{
		Account:            account,
		ClientState:        clientState,
		Filename:           lfn,
		DID:                fmt.Sprintf("cms:%s", lfn),
		EventType:          "get",
		EventVersion:       "API_1.21.6",
		FileReadts:         ts,
		RemoteSite:         site,
		Scope:              "cms",
		Timestamp:          ts,
		TraceTimeentryUnix: ts,
		Usrdn:              usrdn,
		Jobtype:            jobtype,
		Wnname:             wnname,
		StateReason:        gridJobErrorMessage,
	}
	return trc
}
