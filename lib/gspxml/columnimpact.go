package columnimpact

import (
    "encoding/xml"
)

type Column struct {
    Name string `xml:"name,attr"`
}

type Table struct {
    Name string `xml:"name,attr"`
    Columns    []Column `xml:"column"`
}
type RelationNode struct {
    Column string `xml:"column,attr"`
    Coordinate string `xml:"coordinate,attr"`
    Table string `xml:"table,attr"`
}
type Source struct {
    *RelationNode
}
type Target struct {
    *RelationNode
}
type Relation struct {
    Source Source    `xml:"source"`
    Target Target    `xml:"target"`
}
type ColumnImpact struct {
    XMLName xml.Name `xml:"dlineageRelation"`
    Table    []Table  `xml:"table"`
    Relation []Relation `xml:"relation"`
}

func NewColumnImpact(s string) ColumnImpact {
    var res ColumnImpact 

    err := xml.Unmarshal([]byte(s), &res)
    if err != nil {
        panic(err)
    }
    return res
}

func New(vs []string) []ColumnImpact {
    // vs is an array of XML responses from GSP.
    vsm := make([]ColumnImpact, len(vs))
    for i, v := range vs {
        vsm[i] = NewColumnImpact(v)
    }
    return vsm
}
