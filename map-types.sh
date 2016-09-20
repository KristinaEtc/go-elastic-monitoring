#!/bin/bash

curl -XPUT 'http://localhost:9200/global_logs-mapped/table-info/_mapping?pretty' -d @map-types.json \
        --header "Content-Type: application/json"
