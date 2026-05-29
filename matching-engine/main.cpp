#include<bits/stdc++.h>
#include "OrderBook.h"


int main(){

    OrderBook book;
    book.buyOrders.push(Order(1 , 10 , true , 100.00));
    book.buyOrders.push(Order(2 , 10 , true , 105.00));
    book.buyOrders.push(Order(3 , 10 , true , 103.00));

    std::cout <<book.buyOrders.top().price<<std::endl;
    return 0;
}
