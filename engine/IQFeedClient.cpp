#include "platform.h"
#include "IQFeedClient.h"
#include <cstdio>
#include <cstdlib>
#include <cassert>

IQFeedClient::IQFeedClient() {
    plat_net_init();
}

IQFeedClient::~IQFeedClient() { disconnect(); }

bool IQFeedClient::connect(const char* host, uint16_t port) {
    if (_connected.load()) return true;

    _sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_sock == INVALID_SOCK) return false;

    // TCP_NODELAY: disables Nagle's algorithm, which can delay small packets by up to 40 ms.
    int one = 1;
    setsockopt(_sock, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one));

    int rbuf = 1 << 20;
    setsockopt(_sock, SOL_SOCKET, SO_RCVBUF, (char*)&rbuf, sizeof(rbuf));
    plat_socket_latency(_sock);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (::connect(_sock, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
        return false;
    }

    sendRaw("S,SET PROTOCOL,6.2\r\n", 20);

    _lineLen = 0;
    _connected.store(true, std::memory_order_release);

    _recvThread = std::thread([this] { recvLoop(); });
    plat_thread_realtime(_recvThread.native_handle());
    plat_pin_thread(_recvThread.native_handle(), 3);

    return true;
}

void IQFeedClient::disconnect() {
    if (!_connected.exchange(false)) return;
    if (_sock != INVALID_SOCK) {
        plat_shutdown(_sock);
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
    }
    if (_recvThread.joinable())
        _recvThread.join();
}

bool IQFeedClient::watch(const char* symbol) {
    int len = snprintf(_cmdBuf, sizeof(_cmdBuf), "w%s\r\n", symbol);
    return sendRaw(_cmdBuf, len);
}

bool IQFeedClient::unwatch(const char* symbol) {
    int len = snprintf(_cmdBuf, sizeof(_cmdBuf), "r%s\r\n", symbol);
    return sendRaw(_cmdBuf, len);
}

bool IQFeedClient::sendRaw(const char* data, int len) noexcept {
    if (_sock == INVALID_SOCK) return false;
    int sent = 0;
    while (sent < len) {
        int r = ::send(_sock, data + sent, len - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}

void IQFeedClient::recvLoop() noexcept {
    while (_connected.load(std::memory_order_relaxed)) {
        int r = recv(_sock, _recvBuf, (int)sizeof(_recvBuf) - 1, 0);
        if (r <= 0) { _connected.store(false); break; }

        for (int i = 0; i < r; ++i) {
            char c = _recvBuf[i];
            if (c == '\n') {
                if (_lineLen > 0 && _lineBuf[_lineLen - 1] == '\r') --_lineLen;
                _lineBuf[_lineLen] = '\0';
                if (_lineLen > 2)
                    processLine(_lineBuf, _lineLen);
                _lineLen = 0;
            } else if (_lineLen < (int)sizeof(_lineBuf) - 1) {
                _lineBuf[_lineLen++] = c;
            }
        }
    }
}

void IQFeedClient::processLine(const char* line, int /*len*/) noexcept {
    char type = line[0];
    if (type != 'Q' && type != 'P') return;

    const char* fields[18] = {};
    int nf = 0;
    const char* p = line;
    fields[nf++] = p;
    while (*p && nf < 18) {
        if (*p == ',') fields[nf++] = p + 1;
        ++p;
    }
    if (nf < 10) return;

    char sym[48];
    const char* symStart = fields[1];
    const char* symEnd   = fields[2] - 1;
    int symLen = (int)(symEnd - symStart);
    if (symLen <= 0 || symLen >= (int)sizeof(sym)) return;
    memcpy(sym, symStart, symLen);
    sym[symLen] = '\0';

    double last = (fields[2] && *fields[2] != ',') ? atof(fields[2]) : 0.0;
    int    vol  = (nf > 6  && *fields[6] != ',')  ? atoi(fields[6]) : 0;
    double bid  = (nf > 7  && *fields[7] != ',')  ? atof(fields[7]) : 0.0;
    double ask  = (nf > 9  && *fields[9] != ',')  ? atof(fields[9]) : 0.0;

    if (_cb) _cb(sym, bid, ask, last, vol);
}
