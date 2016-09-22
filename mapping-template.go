package main

//const mappingVersion = "1"

var mappingTemplate = `
{
	"template": "global_logs-*",
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
