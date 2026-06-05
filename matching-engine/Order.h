#pragma once
#include <chrono>

struct Order{
    int OrderId, quantity;
    bool IsBuy;
    double price;
    long long timestamp;

    long long t0; // Bot Gen
    long long t1; // Gateway Ingest

    long long seqNo;
    static long long globalSeq;

    Order(int Id, int qty, bool buy, double p, long long t0 = 0, long long t1 = 0);
    Order() : OrderId(0), quantity(0), IsBuy(false), price(0.0), t0(0), t1(0) {}
};
