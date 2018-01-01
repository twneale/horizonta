// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package main

import (
    "runtime/debug"
    "encoding/json"
    "fmt"
    //"strings"

    "github.com/twneale/horizonta/lib"
    "github.com/cskr/pubsub"
)


var err error 


func NewRequestPubsub() *pubsub.PubSub {
    events := pubsub.New(2)
    return events
}

func main() {
    fmt.Println("at 1")
    // Start the event producer.
    tailPubsub := lib.NewDcTail()

    // Start the request aggregator. Create a new pubsub object for it.

    requestPubsub := NewRequestPubsub()
    go lib.StartRequestAggregator(tailPubsub, requestPubsub)

    // Basic console printer.
    go func() {
        var event interface{}
        var ser []byte
        rawevents := tailPubsub.Sub("events")
        for {
            event = <- rawevents
            ser, err = json.MarshalIndent(event, "", "  ")
            if err != nil {
                debug.PrintStack()
                panic(err)
            }
            //if (strings.Contains(string(ser), "RequestsIssued")) || (strings.Contains(string(ser), "RequestsCompleted")) {
            fmt.Println(string(ser))
            //}
        }
    }()

    // Basic console printer.
    go func() {
        fmt.Println("Printing stuff!")
        var event interface{}
        var ser []byte
        rawevents := requestPubsub.Sub("requests")
        for {
            event = <- rawevents
            ser, err = json.MarshalIndent(event, "", "  ")
            if err != nil {
                debug.PrintStack()
                panic(err)
            }
            fmt.Println(string(ser))
        }
    }()

    // Start the fluentd logger.
    go func() {
        lib.StartFluentdLogger(tailPubsub)
    }()

    lib.StartDcTail(tailPubsub)
}
