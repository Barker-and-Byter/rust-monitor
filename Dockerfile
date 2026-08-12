# syntax=docker/dockerfile:1

# start the image with a node
FROM rust:1.95-alpine AS builder

RUN apk add --no-cache musl-dev g++ make

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/rust-monitor   /app/resource_monitor


#Expose the port for the node app to listen on
EXPOSE 3000

CMD ["./resource_monitor"]