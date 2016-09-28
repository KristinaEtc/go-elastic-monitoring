package main

import (
	"bytes"
	"html/template"
)

//const mappingVersion = "1"

var mappingTemplate = `
{
	"template": "{{.template}}",
	"settings": {
		"number_of_shards": 1
	},
	"mappings": {
		"table-info": {
			"properties": {
				"id": {
					"type": "string",
					"index": "not_analyzed"
				},
				"pid": {
					"type": "long"
				},
				"process_short": {
					"type": "string",
					"index": "not_analyzed"
				},
				"process": {
					"type": "string"
				},
				"tid": {
					"type": "long"
				},
				"subtype": {
					"type": "string"
				},
				"user": {
					"type": "string"
				},
				"utc": {
					"type": "date"
				},
				"type": {
					"type": "string",
					"index": "not_analyzed"
				},
				"computer": {
					"type": "string",
					"index": "not_analyzed"
				}
			}
		},
		"aliases": {}
	}
}
`

func initTemplate(templateName string) (string, error) {

	var doc bytes.Buffer

	t := template.New("mapping-template")
	t, err := t.Parse(mappingTemplate)
	if err != nil {
		return "", err
	}
	t.Execute(&doc, templateName)
	mappedTmpl := doc.String()

	return mappedTmpl, nil
}
