#pragma once

#include <cstddef>
#include <cstdint>

struct __attribute__((packed)) TradeEvent {
    uint64_t match_id;
    uint64_t buy_order_id;
    uint64_t sell_order_id;
    uint64_t t0_ns;
    uint64_t t1_ns;
    uint64_t t2_ns;
    double price;
    uint32_t qty;
};

static_assert(sizeof(TradeEvent) == 60, "TradeEvent wire frame must be 60 bytes");
static_assert(offsetof(TradeEvent, qty) == 56, "TradeEvent qty offset must match telemetry protocol");
