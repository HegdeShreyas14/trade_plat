#pragma once
#include "Order.h"

struct BuyComparator{

    bool operator()(const Order& a, const Order& b){
        if(a.price == b.price){
            return a.seqNo > b.seqNo;
        }
        return a.price < b.price;
    }
};
