#!/bin/bash

docker rm -f jobpool-sample
docker run -d \
  --name jobpool-sample \
  -p 4646:4646 \
  -p 4647:4647 \
  -p 4648:4648 \
  -v "/tmp/test:/jobpool/data:rw" \
  -v "/tmp/jobpool/config:/etc/jobpool" \
  jobpool:1.0.0.x86_64 agent