#!/bin/sh

docker build . -t search-solr
docker-compose up
