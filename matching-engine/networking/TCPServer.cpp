#include <iostream>
#include "TCPServer.h"
#include "../MatchingEngine.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <cstring>
#include <sstream>
#include "../Protocol.h"

#define MAX_EVENTS 1024
#define BUFFER_SIZE 8192

namespace {
constexpr const char* WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

uint32_t rol(uint32_t value, int bits) {
    return (value << bits) | (value >> (32 - bits));
}

std::array<uint8_t, 20> sha1(const std::string& input) {
    uint64_t bit_len = static_cast<uint64_t>(input.size()) * 8;
    std::vector<uint8_t> data(input.begin(), input.end());
    data.push_back(0x80);
    while ((data.size() % 64) != 56) data.push_back(0);
    for (int i = 7; i >= 0; --i) data.push_back(static_cast<uint8_t>(bit_len >> (i * 8)));

    uint32_t h0 = 0x67452301;
    uint32_t h1 = 0xEFCDAB89;
    uint32_t h2 = 0x98BADCFE;
    uint32_t h3 = 0x10325476;
    uint32_t h4 = 0xC3D2E1F0;

    for (size_t chunk = 0; chunk < data.size(); chunk += 64) {
        uint32_t w[80]{};
        for (int i = 0; i < 16; ++i) {
            size_t j = chunk + i * 4;
            w[i] = (static_cast<uint32_t>(data[j]) << 24) |
                   (static_cast<uint32_t>(data[j + 1]) << 16) |
                   (static_cast<uint32_t>(data[j + 2]) << 8) |
                   static_cast<uint32_t>(data[j + 3]);
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }

        uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
        for (int i = 0; i < 80; ++i) {
            uint32_t f = 0;
            uint32_t k = 0;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5A827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDC;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6;
            }
            uint32_t temp = rol(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rol(b, 30);
            b = a;
            a = temp;
        }
        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    std::array<uint8_t, 20> digest{};
    uint32_t words[5] = {h0, h1, h2, h3, h4};
    for (int i = 0; i < 5; ++i) {
        digest[i * 4] = static_cast<uint8_t>(words[i] >> 24);
        digest[i * 4 + 1] = static_cast<uint8_t>(words[i] >> 16);
        digest[i * 4 + 2] = static_cast<uint8_t>(words[i] >> 8);
        digest[i * 4 + 3] = static_cast<uint8_t>(words[i]);
    }
    return digest;
}

std::string base64(const uint8_t* data, size_t len) {
    static constexpr char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < len) n |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < len) n |= static_cast<uint32_t>(data[i + 2]);

        out.push_back(table[(n >> 18) & 63]);
        out.push_back(table[(n >> 12) & 63]);
        out.push_back(i + 1 < len ? table[(n >> 6) & 63] : '=');
        out.push_back(i + 2 < len ? table[n & 63] : '=');
    }
    return out;
}

std::string trim(std::string value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string headerValue(const std::string& request, const std::string& header_name) {
    std::istringstream stream(request);
    std::string line;
    const std::string target = lower(header_name);
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        if (lower(line.substr(0, colon)) == target) {
            return trim(line.substr(colon + 1));
        }
    }
    return {};
}

std::string websocketAccept(const std::string& key) {
    auto digest = sha1(key + WS_GUID);
    return base64(digest.data(), digest.size());
}
}

TCPServer::TCPServer(int port, MatchingEngine& engine)
    : port_(port), server_fd_(-1), epoll_fd_(-1), engine_(engine) {}

TCPServer::~TCPServer() {
    stop();
}

void TCPServer::setNonBlocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) return;
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
}

void TCPServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "Failed to create socket." << std::endl;
        return;
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind failed." << std::endl;
        return;
    }

    setNonBlocking(server_fd_); // Ensure accept runs in a non-blocking loop cleanly

    if (listen(server_fd_, SOMAXCONN) < 0) {
        std::cerr << "Listen failed." << std::endl;
        return;
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        std::cerr << "epoll_create1 failed." << std::endl;
        return;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &event) < 0) {
        std::cerr << "epoll_ctl failed for server_fd." << std::endl;
        return;
    }

    running_ = true;
    server_thread_ = std::thread(&TCPServer::run, this);
    std::cout << "WebSocket Gateway listening on port " << port_ << std::endl;
}

void TCPServer::stop() {
    running_ = false;
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    if (server_fd_ >= 0) close(server_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
    server_fd_ = -1;
    epoll_fd_ = -1;
}

void TCPServer::run() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(2, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    epoll_event events[MAX_EVENTS];
    while (running_) {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100); // 100ms timeout blocks so thread isn't spinning
        if (num_events < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            if (events[i].data.fd == server_fd_) {
                int accept_count = 0;
                while (accept_count++ < 256) {
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
                    if (client_fd >= 0) {
                        int flag = 1;
                        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
                        setNonBlocking(client_fd);
                        client_buffers_[client_fd].data.reserve(65536);
                        client_buffers_[client_fd].payload.reserve(65536);

                        epoll_event client_event{};
                        client_event.events = EPOLLIN | EPOLLET; // Edge-triggered
                        client_event.data.fd = client_fd;
                        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &client_event);
                    } else {
                        // EAGAIN means we've accepted all pending connections
                        break; 
                    }
                }
            } else {
                handleClientData(events[i].data.fd);
            }
        }
    }
}

void TCPServer::handleClientData(int client_fd) {
    uint8_t buffer[BUFFER_SIZE];
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer));
        if (bytes_read > 0) {
            auto& rb = client_buffers_[client_fd];
            auto& client_buf = rb.data;
            client_buf.insert(client_buf.end(), buffer, buffer + bytes_read);
            
            constexpr size_t MSG_SIZE = sizeof(OrderMessage);

            auto process_payload_orders = [&]() {
                while (rb.payload.size() - rb.payload_read_ptr >= MSG_SIZE) {
                    long long t1_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();

                    OrderMessage msg;
                    std::memcpy(&msg, rb.payload.data() + rb.payload_read_ptr, MSG_SIZE);

                    bool isBuy = (msg.side == 'B' || msg.side == 'b' || msg.side == 1);
                    engine_.enqueueOrder(Order(msg.order_id, msg.qty, isBuy, msg.price, msg.timestamp_ns, t1_ns));

                    rb.payload_read_ptr += MSG_SIZE;
                }

                if (rb.payload_read_ptr > 16384) {
                    rb.payload.erase(rb.payload.begin(), rb.payload.begin() + rb.payload_read_ptr);
                    rb.payload_read_ptr = 0;
                }
            };

            if (!rb.websocket_ready) {
                const char* header_end = "\r\n\r\n";
                auto end_it = std::search(client_buf.begin(), client_buf.end(), header_end, header_end + 4);
                if (end_it == client_buf.end()) {
                    continue;
                }

                size_t header_size = static_cast<size_t>(std::distance(client_buf.begin(), end_it)) + 4;
                std::string request(client_buf.begin(), client_buf.begin() + header_size);
                std::string key = headerValue(request, "Sec-WebSocket-Key");
                if (key.empty()) {
                    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                    close(client_fd);
                    client_buffers_.erase(client_fd);
                    break;
                }

                std::string response =
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    "Sec-WebSocket-Accept: " + websocketAccept(key) + "\r\n\r\n";
                send(client_fd, response.data(), response.size(), MSG_NOSIGNAL);

                client_buf.erase(client_buf.begin(), client_buf.begin() + header_size);
                rb.read_ptr = 0;
                rb.websocket_ready = true;
            }
            
            bool close_client = false;
            while (client_buf.size() - rb.read_ptr >= 2) {
                const uint8_t* base = client_buf.data() + rb.read_ptr;
                uint8_t opcode = base[0] & 0x0F;
                bool masked = (base[1] & 0x80) != 0;
                uint64_t payload_len = base[1] & 0x7F;
                size_t header_len = 2;

                if (payload_len == 126) {
                    if (client_buf.size() - rb.read_ptr < 4) break;
                    payload_len = (static_cast<uint64_t>(base[2]) << 8) | base[3];
                    header_len = 4;
                } else if (payload_len == 127) {
                    if (client_buf.size() - rb.read_ptr < 10) break;
                    payload_len = 0;
                    for (int i = 0; i < 8; ++i) {
                        payload_len = (payload_len << 8) | base[2 + i];
                    }
                    header_len = 10;
                }

                if (!masked) {
                    close_client = true;
                    break;
                }

                if (payload_len > 1 << 20) {
                    close_client = true;
                    break;
                }

                size_t mask_offset = header_len;
                header_len += 4;
                if (client_buf.size() - rb.read_ptr < header_len + payload_len) break;

                if (opcode == 0x8) {
                    close_client = true;
                    rb.read_ptr += header_len + static_cast<size_t>(payload_len);
                    break;
                }

                if (opcode == 0x2 || opcode == 0x0) {
                    const uint8_t* mask = base + mask_offset;
                    const uint8_t* payload = base + header_len;
                    size_t old_size = rb.payload.size();
                    rb.payload.resize(old_size + static_cast<size_t>(payload_len));
                    for (size_t i = 0; i < payload_len; ++i) {
                        rb.payload[old_size + i] = payload[i] ^ mask[i & 3];
                    }
                    process_payload_orders();
                }

                rb.read_ptr += header_len + static_cast<size_t>(payload_len);
            }
            
            if (rb.read_ptr > 16384) {
                client_buf.erase(client_buf.begin(), client_buf.begin() + rb.read_ptr);
                rb.read_ptr = 0;
            }

            if (close_client) {
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                close(client_fd);
                client_buffers_.erase(client_fd);
                break;
            }
        } else if (bytes_read == 0 || (bytes_read < 0 && errno != EAGAIN)) {
            // Disconnected or error
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
            close(client_fd);
            if (bytes_read < 0) {
                std::cout << "DROPPED client_fd " << client_fd << " err: " << errno << " bytes_read: " << bytes_read << "\n";
            }
            client_buffers_.erase(client_fd);
            break;
        } else {
            // EAGAIN implies we read everything available
            break;
        }
    }
}
