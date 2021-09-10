#! /usr/bin/env bash

set -e

DEFAULT_HSTREAM_DOCKER_TAG="latest"

docker pull hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} || true 

mkdir /tmp/hstream-test-data

docker run \
    -td \
    --rm  \
    --name zookeeper-test \
    --network host \
    zookeeper:3.6

docker run \
    -td \
    --rm  \
    -v /tmp/hstream-test-data:/data/store \
    --name hstore-test \
    --network host \
    hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
    ld-dev-cluster --root /data/store --use-tcp

docker run \
  -td \
  -v /tmp/hstream-test-data:/data/store \
  --name hserver-test \
  --network host \
  hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
  hstream-server --port 6570 --store-config /data/store/logdevice.conf --log-level debug


