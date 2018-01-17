package lib

import (
    "fmt"
    "io/ioutil"
    "encoding/json"
)

type Config struct {
    FluentdHost string
    FluentdPort int
    DisableFluentdPublisher bool 
    GspHost     string
    GspPort     string 
    RedisHost   string
    RedisPort   string
    RedisPassword   string
    RedisDatabase interface{}
    DisableRedisPublisher bool
    PrometheusExporterAddr string
    PrometheusExporterPort string
    DisablePrometheusExporter bool
}

func ParseConfig(configPath string) Config {
    file, err := ioutil.ReadFile(configPath)
    if err != nil {
        fmt.Printf("Error reading config file: %v\n", err)
        panic(err)
    }

    config := Config{
        FluentdHost: "127.0.0.1",
        FluentdPort: 24224,
        DisableFluentdPublisher: false,
        GspHost: "",
        GspPort: "",
        RedisHost: "",
        RedisPort: "6379",
        DisableRedisPublisher: false,
        RedisPassword: "",
        RedisDatabase: 0,
        PrometheusExporterAddr: "0.0.0.0",
        PrometheusExporterPort: "5555",
        DisablePrometheusExporter: false}
    json.Unmarshal(file, &config)
    return config
}
