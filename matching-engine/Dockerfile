# syntax=docker/dockerfile:1

FROM alpine:3.19 AS builder

RUN apk add --no-cache g++ make linux-headers librdkafka-dev

WORKDIR /build

COPY matching-engine ./matching-engine

WORKDIR /build/matching-engine

RUN g++ -std=c++17 -O3 -DNDEBUG -DENABLE_KAFKA -pthread \
    main.cpp MatchingEngine.cpp Order.cpp networking/TCPServer.cpp \
    -lrdkafka++ -lrdkafka -o engine

FROM alpine:3.19 AS runner

RUN apk add --no-cache libstdc++ librdkafka \
    && addgroup -S -g 10001 exchange_group \
    && adduser -S -D -H -u 10001 -G exchange_group exchange_user

WORKDIR /app

COPY --from=builder /build/matching-engine/engine /app/engine

RUN chmod 555 /app/engine \
    && chown -R exchange_user:exchange_group /app

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["/app/engine"]
