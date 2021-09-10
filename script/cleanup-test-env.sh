#! /usr/bin/env bash

set -e

docker stop hstore-test
docker stop hserver-test
docker stop zookeeper-test

rm -rf /tmp/hstream-test-data
