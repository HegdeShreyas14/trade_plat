#include<bits/stdc++.h>
#include "OrderBook.h"
#include "MatchingEngine.h"

int main(){
    MatchingEngine engine;

    engine.addOrder(Order(1,100,true,105)); 
    engine.addOrder(Order(2,50,true,104)); 
    engine.addOrder(Order(3,30,false,110)); 
    engine.addOrder(Order(4,20,false,111)); 

    engine.printOrderBook();

    engine.cancelOrder(2);
    std::cout << "\nAfter cancellation of order ID 2:" << std::endl;
    engine.printOrderBook();
    
    // Add crossing order
    engine.addOrder(Order(5,150,false,105)); 
    std::cout << "\nAfter match:" << std::endl;
    engine.printOrderBook();

    return 0;
}
