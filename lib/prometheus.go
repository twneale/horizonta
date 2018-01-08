package lib

import (
    "fmt"
    "strings"
    "strconv"
    "net/http"
    "text/template"

    "github.com/cskr/pubsub"
)

/*
AllocationPoolStatistics
BlockMemoryManagerStatistics
CatalogPersistenceEvents
ResourcePoolStatus
StorageLayerStatistics
*/

type PrometheusMetric struct {
    Name string
    Value interface{}
    Labels *map[string]string
}



func (m PrometheusMetric) Printf (w http.ResponseWriter) {
    tmpl, err := template.New("metric").Parse(`
      {{- .Name }}{{ "{" }}
      {{- range $key, $value := .Labels }}{{ $key }}={{ $value }},{{ end }}
      {{- "}" }} {{ .Value }}{{ "\n" }}`)
    if err != nil { panic(err) }
    err = tmpl.Execute(w, m)
    if err != nil { panic(err) } 
}

var metrics map[string]PrometheusMetric
var parsedValue interface{}


func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}


func parseValue(s string) interface{} {
    /* Try parsing a value as an int64 or uint64, and if both fail,
    it's a string. */
    var value interface{}
    value = s
    value, err := strconv.ParseInt(s, 10, 64)
    if err != nil {
        value, err := strconv.ParseUint(s, 10, 64)
        if err != nil {
            return s
        }
        return value
    }
    return value
}


func startMetricsAggregator(metrics *map[string]PrometheusMetric, eventsPubsub *pubsub.PubSub) {
    var (
        event   interface{}
        ievent  VerticaEvent
        metric PrometheusMetric 
        metricName string
        labels  map[string]string
    )
    
    reportStreams := []string{
        "ResourcePoolStatus",
        "BlockMemoryManagerStatistics",
        "StorageLayerStatistics",
        "AllocationPoolStatistics",
        "HeartbeatEvents",
        "HeartbeatMonitoringEvents",
    }

    allEvents := eventsPubsub.Sub("events")
    for {
        event = <- allEvents 
        ievent = event.(VerticaEvent) 
        if stringInSlice(ievent.Type, reportStreams) {
            labels = make(map[string]string)
            for k, v := range ievent.Data {
                if k == "time" {
                    continue
                }
                if strings.HasSuffix(k, "_name") || strings.HasSuffix(k, "_id") {
                    labels[k] = ievent.Data[k].(string)
                } else {
                    metricName = fmt.Sprintf("vertica_dc_%s_%s", strings.ToLower(ievent.Type), k)
                    parsedValue = parseValue(v.(string))
                    metric = PrometheusMetric{Name: metricName, Value: v, Labels: &labels}
                    (*metrics)[metric.Name] = metric
                }
            }
        }
    }
}

func handler(w http.ResponseWriter, r *http.Request) {
    for _, v := range metrics {
        v.Printf(w)
    }
}

func startWebServer(config *Config) {
    http.HandleFunc("/metrics", handler)
    bindAddress := fmt.Sprintf("%s:%s", config.PrometheusExporterAddr, config.PrometheusExporterPort)
    http.ListenAndServe(bindAddress, nil)
}

func StartPrometheusExporter(config *Config, eventsPubSub *pubsub.PubSub) {
    metrics = make(map[string]PrometheusMetric)
    go startMetricsAggregator(&metrics, eventsPubSub)
    go startWebServer(config)
}
