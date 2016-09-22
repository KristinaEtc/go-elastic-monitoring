package main

import "time"

// NecessaryFields is a struct with fields which necessary for
// monitoring in Kibana
type NecessaryFields struct {
	// Actually! the field "ProcessName" can be empty. But it's more convenient to parse
	// a struct once, with necessary fields
	ProcessName string `json:"process"`

	ID   string `json:"id"`
	Type string `json:"type"`
	Utc  string `json:"utc"`
}

// time formats
const (
	lenRfc3339WithoutZ     = len("2006-01-02T15:04:05")
	lenRfc3339WithZ        = len("2006-01-02T15:04:05Z")
	lenRfc3339WithTimeZone = len("2006-01-02T15:04:05Z +0300 MSK")
	lenRfc3339             = len(time.RFC3339)
)

//var timeFormats = map[int]string{
//	len("2006-01-02T15:04:05"):  "2006-01-02T15:04:05",
//	len("2006-01-02T15:04:05Z"): "2006-01-02T15:04:05Z",
//	len(time.RFC3339):           time.RFC3339,
//}
