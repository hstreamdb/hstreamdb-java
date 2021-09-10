#! /usr/bin/env zx 


await nothrow($`docker stop hserver-test`);
await nothrow($`docker rm hserver-test`);

await nothrow($`docker stop hstore-test`);
await nothrow($`docker rm hstore-test`);

await nothrow($`docker stop zookeeper-test`);
await nothrow($`docker rm zookeeper-test`);

await $`sudo rm -rf /tmp/hstream-test-data`
