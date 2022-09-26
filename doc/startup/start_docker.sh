#!/bin/bash

docker rm -f jobpool-demo

docker run -d \
  --name jobpool-demo \
  -p 4646:4646 \
  -p 4647:4647 \
  -p 4648:4648 \
  jobpool:1.0.0.x86_64 agent