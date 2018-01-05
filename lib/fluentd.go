package lib

import (
    "fmt"
    "github.com/cskr/pubsub"
    "github.com/fluent/fluent-logger-golang/fluent"
)

var (
    tag     string 
    event   interface{}
    ievent  VerticaEvent
)

func StartFluentdLogger(config *Config, eventsPubsub *pubsub.PubSub) {

    logger, err := fluent.New(fluent.Config{
        FluentPort: config.FluentdPort, 
        FluentHost: config.FluentdHost})
    if err != nil {
        fmt.Println(err)
    }
    defer logger.Close()

    allEvents := eventsPubsub.Sub("events")
    for {
        event = <- allEvents 
        ievent = event.(VerticaEvent) 
        tag = fmt.Sprintf("%s-%s", "vertica-dc", ievent.Type)
        logger.Post(tag, ievent.Data)
    }
}
