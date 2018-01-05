// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package lib


import (
    "runtime/debug"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "time"
    "strconv"

    "github.com/fsnotify/fsnotify"
    "github.com/hpcloud/tail"
    "github.com/cskr/pubsub"
)

var events pubsub.PubSub
var verticaEpoch time.Time
var verticaEpochOffset int


type VerticaEventRaw struct {
    Type string
    Data map[string]string
}

type VerticaEvent struct {
    Type string
    Data map[string]interface{}
}

type VerticaRequest struct {
    Request interface{}
    Result interface{}
}


func handleEvent(raw VerticaEventRaw, metadata map[string]map[string]string, events *pubsub.PubSub) {
    event := parseEvent(raw, metadata)
    publishEvent(event, events)
}


func parseEvent(r VerticaEventRaw, metadata map[string]map[string]string) VerticaEvent {
    tableNameSlug := strings.ToLower(r.Type)
    dataTypes := metadata[tableNameSlug]
    verticaEpoch := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
    unixEpoch := time.Unix(0, 0).Unix()
    verticaEpochOffset := verticaEpoch - unixEpoch 
    newData := make(map[string]interface{})
    for k, v := range r.Data { 
        switch dataTypes[k] {
        // Trying parsing as a signed int64. If that overflows, try unsigned int64.
        case "int":
            var value interface{}
            value, err := strconv.ParseInt(v, 10, 64)
            if err != nil {
                value, err := strconv.ParseUint(v, 10, 64)
                if err != nil {
                    debug.PrintStack()
                    panic(err)
                }
                newData[k] = value
            }
            newData[k] = value
        case "float":
            value, err := strconv.ParseFloat(v, 64)
            if err != nil {
                debug.PrintStack()
                panic(err)
            }
            newData[k] = value
        case "boolean":
            value, err:= strconv.ParseBool(v)
            if err != nil {
                debug.PrintStack()
                panic(err)
            }
            newData[k] = value
        case "timestampz":
            // Behold! The weird way Vertica timestamps must be parsed:
            val, err := strconv.ParseInt(v, 10, 64)
            if err != nil {
                debug.PrintStack()
                panic(err)
            }
            value := time.Unix((val / 1000000) + verticaEpochOffset, 0)
            newData[k] = value
        default:
            newData[k] = v
        }
    }

    return VerticaEvent{Type: r.Type, Data: newData}
}


func publishEvent(event VerticaEvent, events *pubsub.PubSub) {
    events.Pub(event, "events")
}


func tailthing(filename string, created bool, metadata map[string]map[string]string, events *pubsub.PubSub) {

    // Bail if its not a "*.log".
    skipSubstrings := [...]string{"ByDay", "ByHour", "ByMinute", "BySecond"}
    if !strings.HasSuffix(filename, ".log") {
        return
    }
    // Bail if file contains aggregate data.
    for _, s := range skipSubstrings {
        if strings.Contains(filename, s) {
            return
        }
    } 

    // Set the tail config.
    var config tail.Config
    if created {
        config = tail.Config{Follow: true}
    } else {
        location := tail.SeekInfo{Offset: 0, Whence: os.SEEK_END}
        config = tail.Config{Follow: true, Location: &location}
    }

    // Create the tail object.
    t, err := tail.TailFile(filename, config)
    if err != nil {
        log.Fatal(err)
    }

    // If this is a pre-existing file, drop lines until the VerticaEventRaw delimiter is found.
    if created {
        for {
            lineobj := <-t.Lines
            line := strings.TrimSpace(lineobj.Text)
            if line == "." {
                break
            }
        }
    }

    var data map[string]string
    var thing VerticaEventRaw
    for lineobj := range t.Lines {
        line := strings.TrimSpace(lineobj.Text)
        if strings.HasPrefix(line, ":") {
            // Found a VerticaEventRaw key. Start a new VerticaEventRaw.
            data = map[string]string{}
            thing = VerticaEventRaw{Type: strings.Replace(line, ":DC", "", 1), Data: data}
            //_ = thing
        } else if line == "." {
            // Found a VerticaEventRaw delimiter; send the VerticaEventRaw out.
            go handleEvent(thing, metadata, events)
        } else {
            // Found an item; add it to the VerticaEventRaw.
            bits := strings.Split(line, ":")
            key := bits[0]
            value := bits[1]
            data[key] = value
        }
        if err != nil {
            debug.PrintStack()
            panic(err)
        }
    }

    handleEvent(thing, metadata, events)
}


func NewDcTail() *pubsub.PubSub {
    events := pubsub.New(10)
    return events
}


func StartDcTail(events *pubsub.PubSub) {

    // Fetch the DataCollector schema metadata.
    metadata := NewDcMetadata("dc", "DSN=vertica")

    // Watch all data collector files for changes.
    watcher, watch_err := fsnotify.NewWatcher()
    if watch_err != nil {
        log.Fatal(watch_err)
    }

    defer watcher.Close()

    done := make(chan bool)

    go func() {

        files, err := ioutil.ReadDir("./")
        if err != nil {
            log.Fatal(err)
        }
        
        for _, f := range files {
            //log.Println("Tailing existing file:", f.Name())
            go tailthing(f.Name(), false, metadata, events)
        }

        for {
            select {
            case event := <-watcher.Events:
                if event.Op&fsnotify.Create == fsnotify.Create {
                    //log.Println("starting new tail:", event.Name)
                    if event.Op&fsnotify.Create == fsnotify.Create {
                        go tailthing(event.Name, true, metadata, events)
                    }
                }
            case err := <-watcher.Errors:
                log.Println("error:", err)
            }
        }
    }()

    watch_add_err := watcher.Add(".")
    if watch_add_err != nil {
        log.Fatal(watch_add_err)
    }
    <-done
}
