#pragma once

struct Trade
{
    int buyOrderId;
    int sellOrderId;

    int quantity;

    double executionPrice;

    double buyLimitPrice;
    double sellLimitPrice;

    long long timestamp;
};
