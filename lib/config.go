package lib

import (
    "fmt"
    "io/ioutil"
    "encoding/json"
)

type Config struct {
    FluentdHost string
    FluentdPort int
    GspHost     string
    GspPort     string 
    RedisHost   string
    RedisPort   string
    RedisPassword   string
    RedisDatabase interface{}
    PrometheusExporterAddr string
    PrometheusExporterPort string
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
        GspHost: "",
        GspPort: "",
        RedisHost: "",
        RedisPort: "6379",
        RedisPassword: "",
        RedisDatabase: 0,
        PrometheusExporterAddr: "0.0.0.0",
        PrometheusExporterPort: "5555"}
    json.Unmarshal(file, &config)
    return config
}
