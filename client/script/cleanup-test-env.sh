#! /usr/bin/env bash

set -e

docker stop hstore-test
docker stop hserver-test

rm -rf /tmp/hstream-test-data
