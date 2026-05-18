#pragma once
#include "../engine/platform.h"
#include <atomic>
#include <thread>
#include <mutex>
#include <vector>
#include <cstdint>

class EngineProxy;

// -----------------------------------------------------------------------
// HttpServer — minimal HTTP + SSE server for the web UI.
//
// Routes:
//   GET  /              → serve embedded HTML page
//   GET  /api/events    → SSE stream (engine events → browser)
//   POST /api/command   → forward JSON command to EngineProxy
// -----------------------------------------------------------------------
class HttpServer {
public:
    HttpServer(EngineProxy& proxy, uint16_t port = 8080);
    ~HttpServer();

    bool start();
    void stop();

    // Push a JSON event line to all connected SSE subscribers.
    // Thread-safe. Called by EngineProxy::recvLoop().
    void pushEvent(const char* json);

private:
    EngineProxy& _proxy;
    uint16_t     _port;
    socket_t     _listenSock = INVALID_SOCK;
    std::atomic<bool> _running{false};
    std::thread  _acceptThread;

    std::mutex             _sseMux;
    std::vector<socket_t>  _sseClients;

    void acceptLoop();
    void handleClient(socket_t s);

    void serveHtml    (socket_t s);
    void serveEvents  (socket_t s);  // keeps socket open for SSE
    void handleCommand(socket_t s, const char* body, int bodyLen);

    static bool sendAll(socket_t s, const char* data, int len);
};
