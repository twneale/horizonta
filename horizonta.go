// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package main

import (
    "fmt"
    "flag"
    "runtime/debug"
    "encoding/json"

    "github.com/twneale/horizonta/lib"
    "github.com/cskr/pubsub"
)


var err error 


func NewRequestPubsub() *pubsub.PubSub {
    events := pubsub.New(2)
    return events
}

func main() {

    // Get config values.    
    var configPath string
    flag.StringVar(&configPath, "config", "/etc/horizonta/config.json", "Path to json config file")
    flag.Parse()
    config := lib.ParseConfig(configPath)
    fmt.Println("Config is", config)

    // Initialize the event producer.
    tailPubsub := lib.NewDcTail()

    // Start the request aggregator. Create a new pubsub object for it.
    requestPubsub := NewRequestPubsub()
    go lib.StartRequestAggregator(tailPubsub, requestPubsub)

    // Start the fluentd logger.
    go lib.StartFluentdLogger(&config, tailPubsub)

    // Start the prometheus exporter.
    go lib.StartPrometheusExporter(&config, tailPubsub)

    // Start the dc events redis publisher.
    go lib.StartRedisPublisher(&config, tailPubsub, "events")

    // Start the aggregated requests publisher.
    go lib.StartRedisPublisher(&config, requestPubsub, "requests")

    lib.StartDcTail(tailPubsub)
}
