#!/bin/bash

curl -XPUT 'http://192.168.240.105:9200/_template/alert-template?pretty' -d @map-types.json \
        --header "Content-Type: application/json"

#curl -XPUT localhost:9200/_template/template_1?pretty -d  @map-types.json \
#       --header "Content-Type: application/json"

