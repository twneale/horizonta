package lib

import (
    "fmt"
    "strings"
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


func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func startMetricsAggregator(metrics *map[string]PrometheusMetric, eventsPubsub *pubsub.PubSub) {
    var (
        event   interface{}
        ievent  VerticaEvent
        metric PrometheusMetric 
        metricName string
        labels  map[string]string
    )
    if err != nil {
        fmt.Println(err)
    }
    
    reportStreams := []string{
        "ResourcePoolStatus",
        "BlockMemoryManagerStatistics",
        "StorageLayerStatistics"}

    allEvents := eventsPubsub.Sub("events")
    for {
        event = <- allEvents 
        ievent = event.(VerticaEvent) 
        if ievent.Type == "AllocationPoolStatistics" {
            labels = make(map[string]string)
            labels["node_name"] = ievent.Data["node_name"].(string)
            labels["pool_name"] = ievent.Data["pool_name"].(string)
            metric = PrometheusMetric{Name: "vertica_dc_allocationpoolstatistics_chunks", Value: ievent.Data["chunks"], Labels: &labels}
            (*metrics)[metric.Name] = metric
            metric = PrometheusMetric{Name: "vertica_dc_allocationpoolstatistics_total_memory", Value: ievent.Data["total_memory"], Labels: &labels}
            (*metrics)[metric.Name] = metric
            metric = PrometheusMetric{Name: "vertica_dc_allocationpoolstatistics_free_memory", Value: ievent.Data["free_memory"], Labels: &labels}
            (*metrics)[metric.Name] = metric 
        }
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
