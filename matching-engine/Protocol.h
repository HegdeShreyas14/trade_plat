#pragma once
#include <cstddef>
#include <cstdint>

struct OrderMessage {
    uint64_t order_id;      // 8
    uint64_t timestamp_ns;  // 8
    double   price;         // 8
    uint32_t qty;           // 4
    uint8_t  side;          // 1 ('B'=66, 'S'=83)
    uint8_t  padding[3];    // Explicit wire padding to 32 bytes
};

static_assert(sizeof(OrderMessage) == 32, "OrderMessage wire frame must be 32 bytes");
static_assert(offsetof(OrderMessage, side) == 28, "OrderMessage side offset must match bot protocol");
