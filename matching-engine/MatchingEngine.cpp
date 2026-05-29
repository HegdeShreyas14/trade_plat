
#include<chrono>
#include<bits/stdc++.h>
#include "OrderBook.h"
#include "MatchingEngine.h"

void MatchingEngine:: addOrder(const Order& order){
            if(order.IsBuy)
                book.buyOrders.push(order);
            else book.sellOrders.push(order);
         matching();
        }


        void MatchingEngine:: matching(){

            while(!book.buyOrders.empty() && !book.sellOrders.empty() && (book.buyOrders.top().price >= book.sellOrders.top().price)){

                Order buy = book.buyOrders.top();
                Order sell = book.sellOrders.top();

                book.buyOrders.pop();
                book.sellOrders.pop();

                int tradedqty = std::min(buy.quantity,sell.quantity);

                Trade trade;
                trade.buyOrderId = buy.OrderId;       // assign all the values for trade logging
                trade.sellOrderId = sell.OrderId;
                trade.quantity = tradedqty;
                trade.buyLimitPrice = buy.price;
                trade.sellLimitPrice = sell.price;
                trade.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();


                trade.executionPrice = (buy.seqNo < sell.seqNo) ? buy.price : sell.price; // makertaker logic for execution price
                tradeHistory.push_back(trade);


                buy.quantity -= tradedqty;
                sell.quantity -= tradedqty;

                std::cout<< "BUY LOT "<< trade.buyOrderId<< " SELL LOT "<< trade.sellOrderId<< " QTY "<< trade.quantity<< " PRICE "
                         << trade.executionPrice
                         << std::endl;

                if(buy.quantity > 0) book.buyOrders.push(buy);
                if(sell.quantity > 0) book.sellOrders.push(sell);
            }
        }
        void MatchingEngine::cancelOrder(int orderid){

        }
        void MatchingEngine::printTradeHistory(){
                for(const auto& trade : tradeHistory){
                    std::cout<< "BUY "<< trade.buyOrderId<< " SELL "<< trade.sellOrderId<< " QTY "<< trade.quantity<< " PRICE "
                        << trade.executionPrice
                        << std::endl;
                }
            }
