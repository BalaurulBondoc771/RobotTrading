#include "../engine/platform.h"
#include "EngineProxy.h"
#include "HttpServer.h"
#include <cstdio>
#include <cstring>

EngineProxy::EngineProxy(uint16_t enginePort)
    : _enginePort(enginePort) {
    plat_net_init();
}

EngineProxy::~EngineProxy() { disconnect(); }

bool EngineProxy::connect(const char* host) {
    if (_connected.load()) return true;

    _sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_sock == INVALID_SOCK) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(_enginePort);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (::connect(_sock, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
        return false;
    }

    _connected.store(true);
    _recvThread = std::thread([this]{ recvLoop(); });
    return true;
}

void EngineProxy::disconnect() {
    if (!_connected.exchange(false)) return;
    if (_sock != INVALID_SOCK) {
        plat_shutdown(_sock);
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
    }
    if (_recvThread.joinable()) _recvThread.join();
}

bool EngineProxy::sendCommand(const char* json) noexcept {
    if (!_connected.load() || _sock == INVALID_SOCKET) return false;
    // Protocol: each command is one JSON line
    char buf[2048];
    int n = snprintf(buf, sizeof(buf), "%s\n", json);
    if (n <= 0 || n >= (int)sizeof(buf)) return false;
    std::lock_guard<std::mutex> lk(_sendMux);
    int sent = 0;
    while (sent < n) {
        int r = ::send(_sock, buf + sent, n - sent, 0);
        if (r <= 0) { _connected.store(false); return false; }
        sent += r;
    }
    return true;
}

void EngineProxy::recvLoop() noexcept {
    char buf[4096];
    char line[4096];
    int  lineLen = 0;

    while (_connected.load()) {
        int r = recv(_sock, buf, sizeof(buf)-1, 0);
        if (r <= 0) { _connected.store(false); break; }

        for (int i = 0; i < r; ++i) {
            if (buf[i] == '\n') {
                line[lineLen] = '\0';
                if (lineLen > 2 && _http)
                    _http->pushEvent(line);
                lineLen = 0;
            } else if (lineLen < (int)sizeof(line)-1) {
                line[lineLen++] = buf[i];
            }
        }
    }
}
