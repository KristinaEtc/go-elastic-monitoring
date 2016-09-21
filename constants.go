package main

// NecessatyFields is a struct with fields which necessary for
// monitoring in Kibana
type NecessatyFields struct {
	// Actually! the field "ProcessName" can be empty. But it's more convenient to parse
	// a struct once, with necessary fields
	ProcessName string `json:"process"`

	ID   string `json:"id"`
	Type string `json:"type"`
	Utc  string `json:"utc"`
}
