#!/bin/bash

work_path=$(cd "$(dirname "$0")"; pwd)

usage() {
    echo """
    Usage: sh build-jobpool.sh [default | linux | images]
    """
    exit 1
}


function default() {
    echo "start build"
    go build
    echo "copy jobpool to doc/build"
    rm -f doc/build/jobpool
    cp jobpool doc/build/
}


function linux() {
  echo "start build"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build
  echo "copy jobpool to doc/build"
  rm -f doc/build/jobpool
  cp jobpool doc/build/
}

function images() {
    cd doc/build/
    docker rmi -f jobpool:1.0.0.x86_64
    docker build -t jobpool:1.0.0.x86_64 .
}


case "$1" in
  "default")
    default
    ;;
  "linux")
    linux
    ;;
  "images")
    images
    ;;
  *)
    usage
    ;;
esac