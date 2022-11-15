FROM golang:1.12-stretch as build

WORKDIR /app

RUN apt-get update \
 && apt-get install -y ca-certificates  \
 && apt-get install apt-transport-https -y \
 && update-ca-certificates

COPY . .

WORKDIR unsideline/

RUN git config --global http.sslverify false

RUN go build -o bin/unsideline

FROM golang:1.12-stretch as deploy

ENV user "go-dmux"
ENV group "go-dmux"
ENV UID 3119
ENV GID 3000

RUN apt-get update && \
    apt-get install -y procps && \
    apt-get install -y ngrep && \
    apt-get install netcat -y

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/unsideline/bin/unsideline .

RUN groupadd -g ${GID} ${group} && \
    useradd -ms /bin/bash --create-home -g ${GID} -u ${UID} ${user} ; \
    mkdir -p /var/log/unsideline ; \
    chown -R ${user}:${group} /var/log/unsideline ; \
    chmod 777 unsideline; \
    chmod +x unsideline

USER ${user}

ENTRYPOINT ["./unsideline", "/app/config/unsideline-config.json"]
