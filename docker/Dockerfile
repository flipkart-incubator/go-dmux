FROM golang:1.26-bookworm AS build

WORKDIR /app

COPY . .

RUN go build -o bin/go-dmux

FROM debian:bookworm-slim AS deploy

ENV user="go-dmux"
ENV group="go-dmux"
ENV UID=3119
ENV GID=3000

RUN apt-get update && \
    apt-get install -y procps ca-certificates netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/bin/go-dmux .

RUN groupadd -g ${GID} ${group} && \
    useradd -ms /bin/bash --create-home -g ${GID} -u ${UID} ${user} ; \
    mkdir -p /var/log/go-dmux ; \
    chown -R ${user}:${group} /var/log/go-dmux ; \
    chmod 777 go-dmux

USER ${user}

ENTRYPOINT ["./go-dmux"]