#! /usr/bin/env bash

set -e

DEFAULT_HSTREAM_DOCKER_TAG="latest"

docker pull hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} || true 

mkdir /tmp/hstream-test-data

docker run \
    -td \
    --rm  \
    -v /tmp/hstream-test-data:/data/store \
    --name hstore-test \
    -P \
    hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
    ld-dev-cluster --root /data/store --use-tcp

docker run \
  -td \
  --rm \
  -v /tmp/hstream-test-data:/data/store \
  --name hserver-test \
  -p 6570:6570 \
  hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
  hstream-server --port 6570 --store-config /data/store/logdevice.conf
