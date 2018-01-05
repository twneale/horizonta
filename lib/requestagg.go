// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package lib


import (
    "fmt"
    "github.com/cskr/pubsub"
)


func publishAggregatedRequest(event VerticaRequest, ps *pubsub.PubSub) {
    ps.Pub(event, "requests")
}


func StartRequestAggregator(tailPubsub *pubsub.PubSub, requestPubsub *pubsub.PubSub) {
    requestsIssued := make(map[string]interface{})
    var event interface{}
    var ievent VerticaEvent
    var issued interface{}
    var request VerticaRequest
    var cacheKey string 
   
    allEvents := tailPubsub.Sub("events")

    for {
        event = <-allEvents
        ievent = event.(VerticaEvent) 
        switch ievent.Type { 
        case "RequestsIssued":
            cacheKey = fmt.Sprintf("%s::%s", ievent.Data["session_id"], ievent.Data["request_id"])
            requestsIssued[cacheKey] = event
        case "RequestsCompleted":
            cacheKey = fmt.Sprintf("%s::%s", ievent.Data["session_id"], ievent.Data["request_id"])
            issued = requestsIssued[cacheKey]
            request = VerticaRequest{Request: issued, Result: event}
            fmt.Println("About to publish this cow:", request)
            publishAggregatedRequest(request, requestPubsub)
        }
    }
}
