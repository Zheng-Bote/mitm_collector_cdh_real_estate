#!/usr/bin/sh

MITM_VERSION=$(git describe --tags)

CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${MITM_VERSION}" -o ./bin/mitm-collector-cdh-real-estate main.go

cp bin/mitm-collector-cdh-real-estate ../../app/bin/.
