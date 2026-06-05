#include "Order.h"
#include <chrono>

long long Order::globalSeq = 0;

Order::Order(int Id,int qty,bool buy,double p, long long t0_ns, long long t1_ns)
    :
    OrderId(Id),
    quantity(qty),
    IsBuy(buy),
    price(p),
    t0(t0_ns),
    t1(t1_ns)
{
    timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    seqNo= ++globalSeq;
}
