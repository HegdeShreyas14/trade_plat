# syntax=docker/dockerfile:1

FROM alpine:3.19 AS builder

RUN apk add --no-cache g++ make linux-headers

WORKDIR /build

# 1. Copy the local folder into the builder workspace explicitly
COPY matching-engine ./matching-engine

# 2. Step INTO the copied directory where main.cpp actually lives
WORKDIR /build/matching-engine

# 3. Compile using relative paths to this directory
RUN g++ -std=c++17 -O3 -DNDEBUG -pthread \
    main.cpp MatchingEngine.cpp Order.cpp networking/TCPServer.cpp \
    -o engine

FROM alpine:3.19 AS runner

RUN addgroup -S -g 10001 exchange_group \
    && adduser -S -D -H -u 10001 -G exchange_group exchange_user

WORKDIR /app

# 4. Pull the binary explicitly from the subfolder build path
COPY --from=builder /build/matching-engine/engine /app/engine

RUN chmod 555 /app/engine \
    && chown -R exchange_user:exchange_group /app

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["/app/engine"]