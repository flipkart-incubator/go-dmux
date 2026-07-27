#!/bin/bash


docker run --rm  -i -v ~/go:/go golang:1.26-bookworm bash -s <<EOF
cd  src/github.com/flipkart-incubator/go-dmux
export GOPROXY=direct
go build -o docker/go-dmux
EOF
