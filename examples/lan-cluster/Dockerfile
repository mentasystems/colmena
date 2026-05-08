FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/lan-cluster ./examples/lan-cluster

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/lan-cluster /usr/local/bin/lan-cluster
ENTRYPOINT ["/usr/local/bin/lan-cluster"]
