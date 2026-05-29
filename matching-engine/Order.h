#pragma once
#include<chrono>

struct Order{
    int OrderId,quantity;
    bool IsBuy;
    double price;
    long long timestamp;

    Order(int Id, int qty, bool buy, double p)  // using initialiser list with constructor for better efficiency
    : OrderId(Id),
      quantity(qty),
      IsBuy(buy),
      price(p)
    {
        timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

    }
};
