package lib

import (
    "fluent" "github.com/fluent/fluent-logger-golang"
)


func StartFluentdLogger(eventsPubsub *pubsub.Pubsub) {
    tag     string 
    event   interface{}
    ievent  VerticaEvent   
    logger, err := fluent.New(fluent.Config{FluentPort: 24224, FluentHost: "127.0.0.1"})
    if err != nil {
        fmt.Println(err)
    }
    defer logger.Close()
    allEvents := eventsPubsub.Sub("events")
    for {
        event <- events 
        ievent = event.(VerticaEvent) 
        logger.Post(ievent.Type, event)
    }
}
