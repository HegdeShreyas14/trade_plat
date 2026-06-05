#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "OrderBook.h"
#include "MatchingEngine.h"
#include "networking/TCPServer.h"

int main() {
    std::cout << "Starting Trading Benchmark Evaluator Runtime...\n";

    MatchingEngine engine;
    engine.start();

    // Start TCP Epoll Gateway 
    // Passes reference to engine for bot connections
    TCPServer server(8080, engine);
    server.start();

    std::cout << "==========================================\n";
    std::cout << "TCP Server accepting connections on port 8080\n";
    std::cout << "Order string format sent by bots: <ID> <B/S> <QTY> <PRICE>\n";
    std::cout << "Example: 1 B 100 105.0\n";
    std::cout << "Engine is running in headless daemon mode.\n";
    std::cout << "==========================================\n";

    // Run daemonized, loop infinitely
    while(true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "Terminating runtime gracefully...\n";

    // Clean teardown
    server.stop();
    engine.stop();
    
    std::cout << "Final order book state:\n";
    engine.printOrderBook();

    return 0;
}
