#pragma once
#include "platform.h"
#include <atomic>
#include <thread>
#include <functional>
#include <cstring>

// Fired on the IQFeed receive thread (TIME_CRITICAL). Must return quickly — no locks, no alloc.
using IQDataCallback = std::function<void(const char* symbol, double bid, double ask, double last, int vol)>;

class IQFeedClient {
public:
    IQFeedClient();
    ~IQFeedClient();

    bool connect(const char* host = "127.0.0.1", uint16_t port = 5009);
    void disconnect();

    // Symbols must be null-terminated, <= 47 chars (IQFeed OSI option symbols fit in 32 chars)
    bool watch(const char* symbol);
    bool unwatch(const char* symbol);

    void setCallback(IQDataCallback cb) { _cb = std::move(cb); }
    bool isConnected() const noexcept { return _connected.load(std::memory_order_relaxed); }

private:
    socket_t          _sock = INVALID_SOCK;
    std::atomic<bool> _connected{false};
    std::thread       _recvThread;
    IQDataCallback    _cb;

    // Pre-allocated buffers — zero heap alloc in the entire receive hot path
    char _recvBuf[1 << 17]; // 128 KB socket receive buffer (fits many messages)
    char _lineBuf[512];     // single message accumulation (~80 chars typical IQFeed line)
    int  _lineLen = 0;
    char _cmdBuf[64];       // command send buffer (watch/unwatch commands are tiny)

    void recvLoop() noexcept;
    void processLine(const char* line, int len) noexcept;
    bool sendRaw(const char* data, int len) noexcept;
};
