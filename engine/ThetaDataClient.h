#pragma once
#include "platform.h"
#include <functional>
#include <atomic>
#include <thread>
#include <cstdint>

// Same callback signature as the old IQFeedClient — TradingEngine::processIQData unchanged.
using IQDataCallback = std::function<void(const char* sym,
                                          double bid, double ask,
                                          double last, int vol)>;

// -----------------------------------------------------------------------
// ThetaDataClient — real-time quote streaming via ThetaTerminal WebSocket
//
// Setup:
//   1. Install ThetaTerminal from https://thetadata.net
//   2. Launch ThetaTerminal (listens on localhost:25520 by default)
//   3. Active ThetaData subscription required for real-time data
//
// Symbol key convention (used in watch / unwatch):
//   Stock  → plain ticker:  "SPY"
//   Option → compact key:   "{root}{YYYYMMDD}{C|P}{strike_millicents}"
//             e.g.           "SPY20241220C500000"  ($500 call, 2024-12-20)
//             strike_millicents = (int)round(strikeUSD * 1000)
//
// Wire protocol (Theta Terminal v2, verified against
// https://http-docs.thetadata.us/Streaming/Getting-Started.html):
//   Endpoint:    ws://127.0.0.1:25520/v1/events  (single connection only)
//   Subscribe:   {"msg_type":"STREAM","sec_type":"OPTION","req_type":"QUOTE",
//                 "add":true,"id":N,"contract":{"root":"SPY","expiration":20241220,
//                 "strike":500000,"right":"C"}}
//   Unsubscribe: same message with "add":false
//   Quote msg:   {"header":{"type":"QUOTE",...},"contract":{...},"quote":{"bid":..,"ask":..}}
//   Strike unit: 1/10 cent (millicents) — matches our key convention.
// -----------------------------------------------------------------------
class ThetaDataClient {
public:
    ThetaDataClient();
    ~ThetaDataClient();

    bool connect(const char* host = "127.0.0.1", uint16_t port = 25520);
    void disconnect();
    bool isConnected() const noexcept { return _connected.load(std::memory_order_acquire); }
    void setCallback(IQDataCallback cb) { _cb = std::move(cb); }

    bool watch  (const char* key) noexcept;
    bool unwatch(const char* key) noexcept;

    // Fired for non-quote server messages (errors, stream responses) and from
    // connection-level events. May be called from the recv thread — keep it cheap.
    std::function<void(const char* msg)> onLog;

private:
    socket_t          _sock = INVALID_SOCK;
    std::atomic<bool> _connected{false};
    IQDataCallback    _cb;
    std::thread       _recvThread;
    std::atomic<int>  _streamReqId{0};    // "id" field of STREAM requests
    char              _lastNonQuoteType[16] = {}; // dedup for non-quote log lines

    char _frameAccum[1 << 16]; // 64 KB: WebSocket frame accumulation
    int  _frameAccumLen = 0;
    char _msgBuf[1 << 16];     // 64 KB: decoded message text

    bool wsHandshake(const char* host, uint16_t port) noexcept;
    bool wsSendText (const char* json) noexcept;
    bool sendRaw    (const char* data, int len) noexcept;
    void recvLoop   () noexcept;
    void processMessage(const char* msg, int len) noexcept;

    // Zero-alloc JSON field extractors
    static double  jsonDouble(const char* j, int n, const char* key) noexcept;
    static int64_t jsonInt64 (const char* j, int n, const char* key) noexcept;
    static int     jsonString(const char* j, int n, const char* key, char* out, int outSz) noexcept;

    // Decode option key "SPY20241220C500000" into its components.
    // Returns false if key looks like a plain stock ticker.
    static bool parseOptionKey(const char* key,
                                char* root, int rootSz,
                                char* exp,  int expSz,
                                char* right,
                                int*  strikeMillicents) noexcept;

    // Builds into caller-provided buffer — watch/unwatch may be called from
    // multiple control threads concurrently, so no shared member buffer.
    void buildSubscribeJson(char* out, int outSz, const char* key, bool subscribe) noexcept;
};
