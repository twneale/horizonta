package lib

import (
    "fmt"
    "log"
    "time"
    "sync"
    "strings"
    "reflect"
    "net/http"
    "text/template"

    "github.com/cskr/pubsub"
)


type PrometheusMetric struct {
    Name string
    Value interface{}
    Labels *map[string]string
}

type metricTemplateData struct {
    Metric PrometheusMetric
    LabelKeys []string
}


var (
    prometheusMetrics map[string]PrometheusMetric
    prometheusMetricsMutex sync.Mutex
    prometheusEvents map[string]PrometheusMetric
    prometheusEventsMutex sync.Mutex
    dataTypes map[string]string 
    eventsPosted map[string]VerticaEvent
    metric PrometheusMetric 
    labels map[string]string 
)


var templateFunctions = template.FuncMap{
    "last": func(x int, a interface{}) bool {
        return x == reflect.ValueOf(a).Len() - 1
    },
}


func (m PrometheusMetric) getLabelKeys() []string {
    keys := make([]string, 0, len(*m.Labels))
    for k := range *m.Labels {
        keys = append(keys, k)
    } 
    return keys
}

func (m PrometheusMetric) getTemplateData() metricTemplateData {
    keys := m.getLabelKeys()
    return metricTemplateData{Metric: m, LabelKeys: keys}
}   

func (m PrometheusMetric) Printf(w http.ResponseWriter) {
    tmpl, err := template.New("metric").Funcs(templateFunctions).Parse(`
      {{- .Metric.Name }}{{ "{" }}
      {{- range  $i, $k := .LabelKeys -}}
          {{ . }}={{ "\"" }}{{ print (index $.Metric.Labels .) }}{{ "\"" }}{{ if not (last $i $.LabelKeys) }}, {{ end }}
      {{- end -}}
      {{ "}" }} {{ $.Metric.Value }}{{ "\n" }}`)
    if err != nil { panic(err) }
    err = tmpl.Execute(w, m.getTemplateData())
    if err != nil { panic(err) } 
}

func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}


func processMetric(ievent VerticaEvent, dataTypes map[string]string) {
    var (
        metricName string 
    )
    labels := make(map[string]string)

    // Get a lock on the metrics mutex.
    prometheusMetricsMutex.Lock()
    defer prometheusMetricsMutex.Unlock()

    for k, v := range ievent.Data {
        switch dataTypes[k] {
        case "timestampz":
            break
        case "varchar":
            labels[k] = ievent.Data[k].(string)
        default:
            metricName = fmt.Sprintf("vertica_dc_%s_%s", strings.ToLower(ievent.Type), k)
            switch metricValue := v.(type) { 
            case int, uint, int64, uint64, float64:
                metric = PrometheusMetric{Name: metricName, Value: metricValue, Labels: &labels}
                (prometheusMetrics)[metric.Name] = metric 
            }
        }
    }
}

func getMonitoringEventKey(ievent VerticaEvent) string {
    return fmt.Sprintf("%d,%d", ievent.Data["code"], ievent.Data["identifier"])
}

func processEvent(ievent VerticaEvent, dataTypes map[string]string) {
    var (
        key string
        value interface{}
        metricName string 
    )

    labels = make(map[string]string) 
    
    // First handle caching and clearing monitoring events.
    switch ievent.Type {
    case "MonitoringEventsPosted":
        // First purge any expired events from the cache.
        now := time.Now()
        for k, v := range eventsPosted {
            if now.After(v.Data["expiration_timestamp"].(time.Time)) {
                fmt.Println("Expriring a monitoring event:", ievent)
                delete(eventsPosted, k)
            }
        }

        // Then cache this event with the other newly posted and non-expired events.
        key = getMonitoringEventKey(ievent)
        eventsPosted[key] = ievent

    case "MonitoringEventsCleared":
        // Remove explicitly cleared events.
        key = getMonitoringEventKey(ievent)
        delete(eventsPosted, key)
        // Bail here; don't report "cleared" events to prometheus. That will 
        // be reflected by the absense of the posted event in future scrapes.
        return
    }

    if success, ok := ievent.Data["success"]; ok {
        if success.(bool) {
            fmt.Println("Skipping successful heartbeat: ", ievent)
            return
        }
    }

    // Vaule will always be 1, so we can sum them to measure how many events are active.
    value = 1

    // For all streams, put all values in labels and return 
    for k, v := range ievent.Data {
        // First off, avoid timestamps clogging up the Prometheus indexes.
        switch dataTypes[k] {
        case "timestampz":
            continue
        case "varchar":
            if k != "description" {
                labels[k] = v.(string)
            }
        case "int":
            labels[k] = fmt.Sprintf("%d", v.(int64))
        case "float":
            labels[k] = fmt.Sprintf("%f", v.(float64))
        }
    }
    metricName = fmt.Sprintf("vertica_dc_event_%s", strings.ToLower(ievent.Type))
    metric = PrometheusMetric{Name: metricName, Value: value, Labels: &labels}
    prometheusEventsMutex.Lock()
    (prometheusEvents)[metric.Name] = metric
    prometheusEventsMutex.Unlock()
}


func startMetricsAggregator(eventsPubsub *pubsub.PubSub) {
    var (
        event   interface{}
        ievent  VerticaEvent
        tableNameSlug string 
    )

    prometheusMetricsMutex = sync.Mutex{}
    prometheusEventsMutex = sync.Mutex{}

    metricStreams := []string{
        "ResourcePoolStatus",
        "BlockMemoryManagerStatistics",
        "StorageLayerStatistics",
        "AllocationPoolStatistics",
    }

    eventStreams := []string{
        "HeartbeatEvents",
        "HeartbeatMonitoringEvents",
        "MonitoringEventsPosted",
        "MonitoringEventsCleared",
    }

    allEvents := eventsPubsub.Sub("events")
    for {
        event = <- allEvents 
        ievent = event.(VerticaEvent) 
        tableNameSlug = strings.ToLower(ievent.Type)
        dataTypes = metadata[tableNameSlug]

        if stringInSlice(ievent.Type, metricStreams) {
            processMetric(ievent, dataTypes)
        } else if stringInSlice(ievent.Type, eventStreams) {
            processEvent(ievent, dataTypes)
        } 
    }
}

/* -------------------------------------------------------------------------------
 The web server 
------------------------------------------------------------------------------- */
func metricsHandler(w http.ResponseWriter, r *http.Request) {
    for _, v := range prometheusMetrics {
        v.Printf(w)
    }
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
    for _, v := range prometheusEvents {
        if len(*v.Labels) != 0 {
            v.Printf(w)
        }
    }
}


func startWebServer(config *Config) {
    http.HandleFunc("/metrics", metricsHandler)
    http.HandleFunc("/events", eventsHandler)
    bindAddress := fmt.Sprintf("%s:%s", config.PrometheusExporterAddr, config.PrometheusExporterPort)
    http.ListenAndServe(bindAddress, nil)
}

func StartPrometheusExporter(config *Config, eventsPubSub *pubsub.PubSub) {
    if config.DisablePrometheusExporter {
        log.Println("Prometheus exporter disabled per config.")
        return
    }
    prometheusMetrics = make(map[string]PrometheusMetric)
    prometheusEvents = make(map[string]PrometheusMetric)
    eventsPosted = make(map[string]VerticaEvent)
    go startMetricsAggregator(eventsPubSub)
    go startWebServer(config)
}
