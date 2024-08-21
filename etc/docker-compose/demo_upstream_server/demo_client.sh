#!/bin/bash

# should break if any command fails
set -e

# make 1000 requests to the load balancer, should ignore HTTP errors
for i in {1..100}
do
  curl --fail -i -L -X GET http://${LOADBALANCER_HOST}:${LOADBALANCER_PORT}/hello_world || true
done

# fetch stats for the load balancer
curl --fail -i -L -X GET http://${LOADBALANCER_HOST}:${LOADBALANCER_PORT}/_stats
