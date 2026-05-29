#pragma once
#include<chrono>
#include<bits/stdc++.h>
#include "Order.h"
#include "BuyComparator.h"
#include "SellComparator.h"

class OrderBook{

    public:
        std::priority_queue< Order, std::vector<Order>, BuyComparator> buyOrders;

        std::priority_queue< Order, std::vector<Order>, SellComparator> sellOrders;
};
