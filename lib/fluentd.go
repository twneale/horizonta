package lib

import (
    "fmt"
    "log"
    "time"
    "github.com/cskr/pubsub"
    "github.com/fluent/fluent-logger-golang/fluent"
)

var (
    tag     string 
    event   interface{}
    ievent  VerticaEvent
)

func StartFluentdLogger(config *Config, eventsPubsub *pubsub.PubSub) {

    if config.DisableFluentdPublisher {
        log.Println("Fluentd publisher is disabled per config.")
        return
    }

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
        logger.PostWithTime(tag, ievent.Data["time"].(time.Time), ievent.Data)
    }
}
