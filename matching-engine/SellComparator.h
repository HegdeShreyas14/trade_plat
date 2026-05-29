#pragma once
#include "Order.h"

struct SellComparator{

    bool operator()(struct &Order a, struct &Order b){
        if(a.price == b.price){
            return a.timestamp > b.timestamp;
        }
        return a.price > b.price;
    }
};
