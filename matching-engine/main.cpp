#include<bits/stdc++.h>
#include "OrderBook.h"
#include "MatchingEngine.h"


int main(){

    MatchingEngine engine;

    engine.addOrder(
        Order(1,100,true,105));

    engine.addOrder(
        Order(2,30,false,104));

    engine.addOrder(
        Order(3,20,false,105));

    engine.printTradeHistory();
}
