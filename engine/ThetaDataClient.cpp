#include "platform.h"
#include "ThetaDataClient.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cctype>

ThetaDataClient::ThetaDataClient() {
    plat_net_init();
}

ThetaDataClient::~ThetaDataClient() { disconnect(); }

bool ThetaDataClient::connect(const char* host, uint16_t port) {
    if (_connected.load()) return true;

    _sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_sock == INVALID_SOCK) return false;

    int one = 1;
    setsockopt(_sock, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one));
    int rbuf = 1 << 20;
    setsockopt(_sock, SOL_SOCKET, SO_RCVBUF, (char*)&rbuf, sizeof(rbuf));
    plat_socket_latency(_sock);  // SO_BUSY_POLL + TCP_QUICKACK on Linux

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (::connect(_sock, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
        return false;
    }

    if (!wsHandshake(host, port)) {
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
        return false;
    }

    _frameAccumLen = 0;
    _connected.store(true, std::memory_order_release);

    // Join any thread left over from a previous connection that the server
    // dropped (recvLoop clears _connected itself on a socket error), so we
    // never assign over a still-joinable std::thread — which would call
    // std::terminate() on reconnect.
    if (_recvThread.joinable())
        _recvThread.join();
    _recvThread = std::thread([this] { recvLoop(); });
    // TIME_CRITICAL / SCHED_FIFO 99: market data must never be preempted.
    plat_thread_realtime(_recvThread.native_handle());
    // Pin to core 3: prevents cache eviction from core migration (~1 µs jitter each).
    // Adjust to a dedicated core that doesn't share with the IBKR trade thread (core 2).
    plat_pin_thread(_recvThread.native_handle(), 3);

    return true;
}

void ThetaDataClient::disconnect() {
    // NOTE: do not early-return based on _connected. When the server closes the
    // connection, recvLoop() sets _connected=false itself and exits, leaving
    // _recvThread joinable. Returning here would skip the join() below and the
    // thread would be destroyed while joinable -> std::terminate(). Always run
    // the close + join; both steps are idempotent on repeated calls.
    _connected.store(false);
    if (_sock != INVALID_SOCK) {
        plat_shutdown(_sock);
        plat_close_socket(_sock);
        _sock = INVALID_SOCK;
    }
    if (_recvThread.joinable())
        _recvThread.join();
}

// RFC 6455 WebSocket opening handshake — client side only.
// We use a fixed Sec-WebSocket-Key; the server's Accept header is not validated
// since we're a dedicated single-connection client.
bool ThetaDataClient::wsHandshake(const char* host, uint16_t port) noexcept {
    char req[512];
    snprintf(req, sizeof(req),
        "GET / HTTP/1.1\r\n"
        "Host: %s:%d\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: ZeYfLSMHXU0kqANgKLiqfg==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n",
        host, (int)port);
    if (!sendRaw(req, (int)strlen(req))) return false;

    // Read server response until \r\n\r\n
    char resp[512] = {};
    int  n = 0;
    while (n < (int)sizeof(resp) - 1) {
        int r = recv(_sock, resp + n, 1, 0);
        if (r <= 0) return false;
        ++n;
        if (n >= 4 && memcmp(resp + n - 4, "\r\n\r\n", 4) == 0) break;
    }
    return strstr(resp, "101") != nullptr;
}

// Send a masked WebSocket text frame.
// Uses a 520-byte stack array so _cmdBuf stays available for callers.
bool ThetaDataClient::wsSendText(const char* json) noexcept {
    int payLen = (int)strlen(json);
    if (payLen <= 0 || payLen > 510) return false;

    uint8_t frame[520];
    int hdr = 0;
    frame[hdr++] = 0x81; // FIN=1, opcode=text
    if (payLen <= 125) {
        frame[hdr++] = 0x80 | (uint8_t)payLen;
    } else {
        frame[hdr++] = 0xFE; // MASK=1, ext-16 length
        frame[hdr++] = (uint8_t)(payLen >> 8);
        frame[hdr++] = (uint8_t)(payLen & 0xFF);
    }
    const uint8_t mask[4] = {0x5A, 0xA5, 0xC3, 0x3C};
    frame[hdr++] = mask[0]; frame[hdr++] = mask[1];
    frame[hdr++] = mask[2]; frame[hdr++] = mask[3];
    for (int i = 0; i < payLen; ++i)
        frame[hdr + i] = (uint8_t)json[i] ^ mask[i & 3];
    return sendRaw((char*)frame, hdr + payLen);
}

bool ThetaDataClient::sendRaw(const char* data, int len) noexcept {
    if (_sock == INVALID_SOCK) return false;
    int sent = 0;
    while (sent < len) {
        int r = ::send(_sock, data + sent, len - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}

bool ThetaDataClient::parseOptionKey(const char* key,
                                      char* root, int rootSz,
                                      char* exp,  int expSz,
                                      char* right,
                                      int*  strikeMillicents) noexcept {
    const char* p = key;

    // Root: leading non-digit chars (e.g. "SPY", "NVDA")
    int ri = 0;
    while (*p && !isdigit((unsigned char)*p) && ri < rootSz - 1)
        root[ri++] = *p++;
    root[ri] = '\0';
    if (ri == 0 || !isdigit((unsigned char)*p)) return false;

    // Date: exactly 8 digits (YYYYMMDD)
    int ei = 0;
    while (isdigit((unsigned char)*p) && ei < expSz - 1)
        exp[ei++] = *p++;
    exp[ei] = '\0';
    if (ei != 8) return false;

    // Right: C or P
    if (*p != 'C' && *p != 'P') return false;
    *right = *p++;

    // Strike: remaining digits in millicents
    if (!isdigit((unsigned char)*p)) return false;
    *strikeMillicents = atoi(p);
    return true;
}

void ThetaDataClient::buildSubscribeJson(const char* key, bool subscribe) noexcept {
    // TODO: verify "STREAM"/"STOP" msg_type values against ThetaData docs.
    // Some versions may use "subscribe"/"unsubscribe".
    const char* msgType = subscribe ? "STREAM" : "STOP";
    char root[16], exp[9], right;
    int  strikeMC = 0;

    if (parseOptionKey(key, root, sizeof(root), exp, sizeof(exp), &right, &strikeMC)) {
        snprintf(_cmdBuf, sizeof(_cmdBuf),
            "{\"msg_type\":\"%s\",\"sec_type\":\"OPTION\",\"req_type\":\"QUOTE\","
            "\"root\":\"%s\",\"exp\":%s,\"strike\":%d,\"right\":\"%c\"}",
            msgType, root, exp, strikeMC, right);
    } else {
        snprintf(_cmdBuf, sizeof(_cmdBuf),
            "{\"msg_type\":\"%s\",\"sec_type\":\"STOCK\",\"req_type\":\"QUOTE\","
            "\"ticker\":\"%s\"}",
            msgType, key);
    }
}

bool ThetaDataClient::watch(const char* key) noexcept {
    buildSubscribeJson(key, true);
    return wsSendText(_cmdBuf);
}

bool ThetaDataClient::unwatch(const char* key) noexcept {
    buildSubscribeJson(key, false);
    return wsSendText(_cmdBuf);
}

// ============================================================
// HOT PATH: WebSocket frame receiver — TIME_CRITICAL thread, core 3.
// No heap allocation, no locks.
// ============================================================
void ThetaDataClient::recvLoop() noexcept {
    int acc = 0; // accumulated bytes in _frameAccum

    while (_connected.load(std::memory_order_relaxed)) {
        int space = (int)sizeof(_frameAccum) - acc - 1;
        if (space <= 0) { acc = 0; continue; }

        int r = recv(_sock, _frameAccum + acc, space, 0);
        if (r <= 0) { _connected.store(false); break; }
        acc += r;

        int consumed = 0;
        while (acc - consumed >= 2) {
            const uint8_t* p     = (uint8_t*)_frameAccum + consumed;
            int            avail = acc - consumed;

            uint8_t opcode = p[0] & 0x0F;
            bool    masked = (p[1] & 0x80) != 0;
            int     payLen = p[1] & 0x7F;
            int     hdrLen = 2;

            if (payLen == 126) {
                if (avail < 4) break;
                payLen = ((int)p[2] << 8) | p[3];
                hdrLen = 4;
            } else if (payLen == 127) {
                if (avail < 10) break;
                payLen = (int)(((uint64_t)p[2]<<56)|((uint64_t)p[3]<<48)|
                               ((uint64_t)p[4]<<40)|((uint64_t)p[5]<<32)|
                               ((uint64_t)p[6]<<24)|((uint64_t)p[7]<<16)|
                               ((uint64_t)p[8]<<8) |(uint64_t)p[9]);
                hdrLen = 10;
            }

            int totalLen = hdrLen + (masked ? 4 : 0) + payLen;
            if (avail < totalLen) break; // incomplete frame, wait for more data

            if (opcode == 0x01 || opcode == 0x00 || opcode == 0x02) {
                // Text / continuation / binary
                const char*    payload = (char*)p + hdrLen + (masked ? 4 : 0);
                const uint8_t* maskKey = masked ? p + hdrLen : nullptr;
                int msgLen = (payLen < (int)sizeof(_msgBuf) - 1) ? payLen : (int)sizeof(_msgBuf) - 1;

                if (masked && maskKey) {
                    for (int i = 0; i < msgLen; ++i)
                        _msgBuf[i] = payload[i] ^ maskKey[i & 3];
                } else {
                    memcpy(_msgBuf, payload, msgLen);
                }
                _msgBuf[msgLen] = '\0';
                processMessage(_msgBuf, msgLen);

            } else if (opcode == 0x09) {
                // Ping → Pong (masked, empty payload)
                const uint8_t pong[6] = {0x8A, 0x80, 0, 0, 0, 0};
                sendRaw((char*)pong, 6);

            } else if (opcode == 0x08) {
                // Close frame
                _connected.store(false);
                break;
            }

            consumed += totalLen;
        }

        if (consumed > 0 && consumed < acc)
            memmove(_frameAccum, _frameAccum + consumed, acc - consumed);
        acc -= consumed;
    }
}

// ============================================================
// Fast numeric parsers — no locale, no exponent, no heap.
// Handle "-?[0-9]+(\.[0-9]+)?" (prices) and "-?[0-9]+" (integers).
// ~10-30 ns each vs ~100-300 ns for atof/atoll on MSVC.
// ============================================================

static double fast_atof(const char* p, const char* end) noexcept {
    if (p >= end) return 0.0;
    bool neg = (*p == '-');
    if (neg && ++p >= end) return 0.0;
    int64_t intPart = 0;
    while (p < end && (unsigned)(*p - '0') < 10u)
        intPart = intPart * 10 + (*p++ - '0');
    if (p >= end || *p != '.') return neg ? -(double)intPart : (double)intPart;
    ++p;
    int64_t fracPart = 0; int fracDig = 0;
    while (p < end && (unsigned)(*p - '0') < 10u)
        { fracPart = fracPart * 10 + (*p++ - '0'); ++fracDig; }
    static const double pow10[10] = {1,10,100,1e3,1e4,1e5,1e6,1e7,1e8,1e9};
    double frac = (fracDig > 0 && fracDig < 10) ? fracPart / pow10[fracDig] : 0.0;
    double val  = (double)intPart + frac;
    return neg ? -val : val;
}

static int64_t fast_atoll(const char* p, const char* end) noexcept {
    if (p >= end) return 0;
    bool neg = (*p == '-');
    if (neg && ++p >= end) return 0;
    int64_t v = 0;
    while (p < end && (unsigned)(*p - '0') < 10u)
        v = v * 10 + (*p++ - '0');
    return neg ? -v : v;
}

// Write unsigned 64-bit integer into buf (no null terminator). Returns chars written.
static inline int fast_u64toa(char* buf, uint64_t v) noexcept {
    if (v == 0) { buf[0] = '0'; return 1; }
    char tmp[20]; int tl = 0;
    while (v) { tmp[tl++] = (char)('0' + v % 10); v /= 10; }
    for (int i = 0; i < tl; ++i) buf[i] = tmp[tl - 1 - i];
    return tl;
}

// ============================================================
// Parse a ThetaData QUOTE JSON message, fire _cb on the hot path.
// Single-pass parser: one walk extracts all fields simultaneously
// instead of calling findValue() once per field (was 9 linear scans).
// ============================================================
void ThetaDataClient::processMessage(const char* msg, int len) noexcept {
    char    typeStr[16] = {};
    char    root[16]    = {};
    char    ticker[16]  = {};
    char    rightStr[4] = {};
    double  bid  = 0, ask = 0, last = 0;
    int     bidSz = 0;
    int64_t exp = 0, strike = 0;

    const char* p   = msg;
    const char* end = msg + len;

    while (p < end) {
        // find next key opening quote
        while (p < end && *p != '"') ++p;
        if (++p >= end) break;

        const char* ks = p;
        while (p < end && *p != '"') ++p;
        if (p >= end) break;
        int kl = (int)(p - ks);
        if (++p >= end) break;

        // skip ':' and spaces
        while (p < end && (*p == ':' || *p == ' ')) ++p;
        if (p >= end) break;

        if (*p == '"') {
            // --- string value ---
            const char* vs = ++p;
            while (p < end && *p != '"') ++p;
            int vl = (int)(p - vs);
            if (p < end) ++p;

            switch (kl) {
            case 4:
                if (memcmp(ks, "type", 4) == 0) {
                    int n = vl < 15 ? vl : 15;
                    memcpy(typeStr, vs, n); typeStr[n] = '\0';
                } else if (memcmp(ks, "root", 4) == 0) {
                    int n = vl < 15 ? vl : 15;
                    memcpy(root, vs, n); root[n] = '\0';
                }
                break;
            case 5:
                if (memcmp(ks, "right", 5) == 0)
                    rightStr[0] = vl > 0 ? vs[0] : 0, rightStr[1] = '\0';
                break;
            case 6:
                if (memcmp(ks, "ticker", 6) == 0) {
                    int n = vl < 15 ? vl : 15;
                    memcpy(ticker, vs, n); ticker[n] = '\0';
                }
                break;
            }
        } else {
            // --- numeric value — ends at ',' '}' or whitespace ---
            const char* vs = p;
            while (p < end && *p != ',' && *p != '}' && *p != ' ') ++p;

            switch (kl) {
            case 3:
                if      (memcmp(ks, "bid", 3) == 0) bid    = fast_atof(vs, p);
                else if (memcmp(ks, "ask", 3) == 0) ask    = fast_atof(vs, p);
                else if (memcmp(ks, "exp", 3) == 0) exp    = fast_atoll(vs, p);
                break;
            case 4:
                if (memcmp(ks, "last", 4) == 0) last = fast_atof(vs, p);
                break;
            case 6:
                if (memcmp(ks, "strike", 6) == 0) strike = fast_atoll(vs, p);
                break;
            case 8:
                if (memcmp(ks, "bid_size", 8) == 0) bidSz = (int)fast_atoll(vs, p);
                break;
            case 10:
                if (memcmp(ks, "last_price", 10) == 0) last = fast_atof(vs, p);
                break;
            }
        }
    }

    if (strcmp(typeStr, "QUOTE") != 0) return;

    // "root" takes priority; fall back to "ticker" for stock-only feeds
    if (root[0] == '\0' && ticker[0] != '\0')
        memcpy(root, ticker, sizeof(root));

    char r0 = rightStr[0];
    if (r0 == 'C' || r0 == 'P' || r0 == 'c' || r0 == 'p') {
        if (root[0] == '\0') return;
        char rch = (r0 | 0x20) == 'c' ? 'C' : 'P'; // normalise case
        char sym[64]; int n = 0;
        for (const char* r = root; *r; ) sym[n++] = *r++;
        n += fast_u64toa(sym + n, (uint64_t)exp);
        sym[n++] = rch;
        n += fast_u64toa(sym + n, (uint64_t)strike);
        sym[n] = '\0';
        if (_cb) _cb(sym, bid, ask, last, bidSz);
    } else {
        if (root[0] == '\0') return;
        if (_cb) _cb(root, bid, ask, last, bidSz);
    }
}

// ============================================================
// JSON field helpers — linear scan, no heap, no DOM parser.
// ============================================================

static const char* findValue(const char* j, int n, const char* key) noexcept {
    int klen = (int)strlen(key);
    for (int i = 0; i + klen + 2 < n; ++i) {
        if (j[i] == '"' &&
            memcmp(j + i + 1, key, klen) == 0 &&
            j[i + 1 + klen] == '"') {
            int k = i + klen + 2;
            while (k < n && (j[k] == ':' || j[k] == ' ')) ++k;
            if (k < n) return j + k;
        }
    }
    return nullptr;
}

double ThetaDataClient::jsonDouble(const char* j, int n, const char* key) noexcept {
    const char* v = findValue(j, n, key);
    return v ? atof(v) : 0.0;
}

int64_t ThetaDataClient::jsonInt64(const char* j, int n, const char* key) noexcept {
    const char* v = findValue(j, n, key);
    return v ? (int64_t)atoll(v) : 0LL;
}

int ThetaDataClient::jsonString(const char* j, int n, const char* key,
                                 char* out, int outSz) noexcept {
    const char* v = findValue(j, n, key);
    if (!v || *v != '"') return 0;
    ++v;
    int i = 0;
    while (*v && *v != '"' && i < outSz - 1)
        out[i++] = *v++;
    out[i] = '\0';
    return i;
}
