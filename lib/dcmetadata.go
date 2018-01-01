// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !plan9

package lib

import (
    "log"
    "strings"
    "runtime/debug"
    "database/sql"

    _ "github.com/alexbrainman/odbc"
)


var (
    err         error 
    tableName   string
    columnName  string
    dataType    string
)


var metadata map[string]map[string]string


func NewDcMetadata(dcSchemaName string, dataSourceName string) map[string]map[string]string {

    var db *sql.DB
    db, err = sql.Open("odbc", dataSourceName)
    if err != nil {
        debug.PrintStack()
        panic(err)
    }

    rows, err := db.Query(`
      select table_name, column_name, data_type
      from v_catalog.columns
      where table_schema = 'dc'`)

    if err != nil {
        log.Fatal(err)
    }

    defer rows.Close()

    metadata = make(map[string]map[string]string)
    var tablemap map[string]string
    var tableNameSlug string
    for rows.Next() {
        err := rows.Scan(&tableName, &columnName, &dataType)
        if err != nil {
            log.Fatal(err)
        }

        // Slice off the parenthesized varchar length.
        bits := strings.Split(dataType, "(")
        dataType = bits[0]

        // Slugify tablename.
        tableNameSlug = strings.Replace(tableName, "_", "", -1)
        tableNameSlug = strings.Replace(tableNameSlug, "dc", "", 1)
        tablemap = metadata[tableNameSlug]
        if tablemap == nil {
            tablemap = map[string]string{columnName: dataType}
            metadata[tableNameSlug] = tablemap
        } else {
            tablemap[columnName] = dataType
        }
    }

    err = rows.Err()
    if err != nil {
        log.Fatal(err)
    }

    return metadata
}

