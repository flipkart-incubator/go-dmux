#!/bin/bash


docker run --rm  -i -v ~/go:/go golang:1.12-stretch bash -s <<EOF
cd  src/github.com/flipkart-incubator/go-dmux
export GOPROXY=direct
export GO111MODULE=on
export GOSUMDB=off
go build -mod=vendor -o docker/go-dmux
#go mod vendor -v
#go build
EOF
