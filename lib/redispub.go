
package lib

import (
    "fmt"
    "log"
    "encoding/json"
    "runtime/debug"

    "github.com/cskr/pubsub"
    "github.com/go-redis/redis"
)


func StartRedisPublisher(config *Config, pubSub *pubsub.PubSub, channel string) {
    // Bail if no redis host specified.
    if config.DisableRedisPublisher {
        log.Println("Redis publisher disabled per config.")
        return
    }
    bindAddress := fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort)
    client := redis.NewClient(&redis.Options{
        Addr:     bindAddress,
        Password: config.RedisPassword,
        DB:       0,
    })
    var (
        event   interface{}
        ser []byte
    )
    allEvents := pubSub.Sub(channel)
    for {
        event = <- allEvents 
        ser, err = json.MarshalIndent(event, "", "  ")
        if err != nil {
            debug.PrintStack()
            fmt.Println("BAD EVENT: ", event)
            panic(err)
        }
        err = client.Publish(channel, string(ser)).Err()
        if err != nil {
            panic(err)
        }
    } 
}
