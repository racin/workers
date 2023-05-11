FROM golang:1.17

WORKDIR /src
# COPY . ./
# COPY [^workers]* ./
COPY *.go ./
COPY go.* ./
COPY Makefile ./

RUN make

ENTRYPOINT ["bin/workers", "--"]
EXPOSE 8080
# EXPOSE 8080 8081 8082