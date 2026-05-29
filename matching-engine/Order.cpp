#include "Order.h"

#include <chrono>

long long Order::globalSeq = 0;

Order::Order(int Id,int qty,bool buy,double p)   // using initialiser list with constructor for better efficiency
    :
    OrderId(Id),
    quantity(qty),
    IsBuy(buy),
    price(p)
{
    timestamp =
        std::chrono::duration_cast<
            std::chrono::milliseconds>(
                std::chrono::system_clock::now()
                    .time_since_epoch()).count();

    seqNo= ++globalSeq;
}
