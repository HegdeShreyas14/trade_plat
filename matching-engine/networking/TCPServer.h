#pragma once
#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include <unordered_map>

class MatchingEngine;

class TCPServer {
public:
    TCPServer(int port, MatchingEngine& engine);
    ~TCPServer();

    void start();
    void stop();

private:
    void run();
    void setNonBlocking(int sockfd);
    void handleClientData(int client_fd);

    int port_;
    int server_fd_;
    int epoll_fd_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    MatchingEngine& engine_;

    struct RingBuffer {
        std::vector<uint8_t> data;
        size_t read_ptr{0};
        std::vector<uint8_t> payload;
        size_t payload_read_ptr{0};
        bool websocket_ready{false};
    };

    std::unordered_map<int, RingBuffer> client_buffers_;
};
