#!/bin/bash

curl -XPUT 'http://localhost:9200/global_logs-*/table-info/_mapping?pretty' -d @map-types.json \
        --header "Content-Type: application/json"

#curl -XPUT localhost:9200/_template/template_1?pretty -d  @map-types.json \
#       --header "Content-Type: application/json"

