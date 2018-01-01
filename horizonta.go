// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package main

import (
    "runtime/debug"
    "encoding/json"
    "fmt"
    "strings"

    "./horizonta"
)


var err error 

func main() {
    // Start the event producer.
    pubsub := dctail.New()
    go dctail.Start(pubsub)

    // Start the request aggregator.
    go requestagg.Start(pubsub)

    // Basic console printer.
    var event interface{}
    var ser []byte
    rawevents := pubsub.Sub("events", "requests")
    for {
        event = <- rawevents
        ser, err = json.MarshalIndent(event, "", "  ")
        if err != nil {
            debug.PrintStack()
            panic(err)
        }
        if (strings.Contains(string(ser), "RequestsIssued")) || (strings.Contains(string(ser), "RequestsCompleted")) {
            fmt.Println(string(ser))
        }
    }

}
