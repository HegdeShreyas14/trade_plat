#pragma once
#include<chrono>

struct Order{
    int OrderId,quantity;
    bool IsBuy;
    double price;
    long long timestamp;

    long long seqNo;
    static long long globalSeq;

    Order(int Id, int qty, bool buy, double p);
};
