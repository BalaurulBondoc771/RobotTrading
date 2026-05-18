#pragma once
#include "platform.h"
#include <atomic>
#include <thread>
#include <mutex>
#include <vector>
#include <string>

class TradingEngine;

// -----------------------------------------------------------------------
// ControlServer — exposes TradingEngine over a local TCP socket.
//
// Protocol: newline-delimited JSON, both directions.
//   In  (client → engine):  {"cmd":"start"}\n
//   Out (engine → client):  {"type":"log","msg":"..."}\n
//
// All threads run at BELOW_NORMAL priority — zero impact on hot path.
// Default port: 7799 (internal IPC; not exposed to the internet).
// -----------------------------------------------------------------------
class ControlServer {
public:
    explicit ControlServer(TradingEngine& eng, uint16_t port = 7799);
    ~ControlServer();

    bool start();
    void stop();
    bool isRunning() const noexcept { return _running.load(); }

    // Thread-safe: may be called from any engine thread.
    void broadcastLog(const char* msg, bool isErr = false);

private:
    TradingEngine& _eng;
    uint16_t       _port;
    socket_t       _listenSock = INVALID_SOCK;
    std::atomic<bool> _running{false};
    std::thread    _acceptThread;
    std::thread    _statusThread;

    std::mutex               _clientsMux;
    std::vector<socket_t>    _clients;

    int _lastExpVersion   = -1;
    int _lastChainVersion = -1;

    void acceptLoop();
    void clientLoop(socket_t s);
    void statusLoop();

    void handleCommand(const char* json, int len);
    void applyParam(const char* key, double value);

    void broadcast(const char* json);         // caller must hold _clientsMux
    void broadcastStatus();
    void broadcastExpirations();
    void broadcastChain();
};
