#pragma once
#include<chrono>
#include<map>
#include<list>
#include<unordered_map>
#include "Order.h"

class OrderBook{
    public:
        std::map<double, std::list<Order>, std::greater<double>> buyOrders;
        std::map<double, std::list<Order>, std::less<double>> sellOrders;

        struct OrderLocation {
            bool isBuy;
            double price;
            std::list<Order>::iterator iterator;
        };

        std::unordered_map<int, OrderLocation> orderMap;
};
