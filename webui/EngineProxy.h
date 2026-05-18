#pragma once
#include "../engine/platform.h"
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>

class HttpServer;

// -----------------------------------------------------------------------
// EngineProxy — connects to the engine's ControlServer (localhost:7799).
// Forwards events (engine → HttpServer → browser) and
// commands (browser → HttpServer → engine).
// -----------------------------------------------------------------------
class EngineProxy {
public:
    explicit EngineProxy(uint16_t enginePort = 7799);
    ~EngineProxy();

    void setHttpServer(HttpServer* s) { _http = s; }

    bool connect(const char* host = "127.0.0.1");
    void disconnect();
    bool isConnected() const noexcept { return _connected.load(); }

    // Thread-safe: called from HTTP handler threads.
    bool sendCommand(const char* json) noexcept;

private:
    uint16_t          _enginePort;
    socket_t          _sock = INVALID_SOCK;
    std::atomic<bool> _connected{false};
    HttpServer*       _http = nullptr;
    std::thread       _recvThread;
    std::mutex        _sendMux;

    void recvLoop() noexcept;
};
