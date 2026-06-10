// ============================================================
// test_thetadata.cpp — end-to-end test of ThetaDataClient against
// a mock Theta Terminal v2 WebSocket server on localhost.
//
// Covers:
//   - handshake path (/v1/events) and 101 upgrade
//   - subscribe JSON format (msg_type STREAM, add:true/false, nested contract,
//     expiration/strike/right fields, millicent strikes)
//   - server->client frame parsing: single frame, fragmented message (RFC6455
//     continuation), ping->pong, 16-bit extended length
//   - quote parsing of the real v2 nested header/contract/quote layout
//   - non-quote messages do not crash or produce callbacks
//
// No test framework: prints failures, exit code != 0 on any failure.
// Needs no IBApi / protobuf — links only ThetaDataClient + platform sockets.
// ============================================================

#include "../engine/platform.h"
#include "../engine/ThetaDataClient.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <atomic>

static int g_failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { ++g_failures; fprintf(stderr, "FAIL: %s (line %d)\n", msg, __LINE__); } \
} while (0)

// ------------------------------------------------------------
// Collected quote callbacks
// ------------------------------------------------------------
struct Quote {
    std::string sym;
    double bid, ask, last;
    int vol;
};
static std::mutex         g_qMux;
static std::vector<Quote> g_quotes;

// ------------------------------------------------------------
// Socket helpers (test side)
// ------------------------------------------------------------
static void setRecvTimeout(socket_t s, int ms) {
#ifdef _WIN32
    DWORD t = (DWORD)ms;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&t, sizeof(t));
#else
    timeval tv{ ms / 1000, (ms % 1000) * 1000 };
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv, sizeof(tv));
#endif
}

static bool sendAll(socket_t s, const char* d, int n) {
    int sent = 0;
    while (sent < n) {
        int r = ::send(s, d + sent, n - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}

// Send a server->client text frame (unmasked). fin=false emits a fragment.
static bool sendTextFrame(socket_t s, const char* payload, int len,
                          bool fin = true, bool continuation = false) {
    unsigned char hdr[4];
    int h = 0;
    hdr[h++] = (unsigned char)((fin ? 0x80 : 0x00) | (continuation ? 0x00 : 0x01));
    if (len <= 125) {
        hdr[h++] = (unsigned char)len;
    } else {
        hdr[h++] = 126;
        hdr[h++] = (unsigned char)(len >> 8);
        hdr[h++] = (unsigned char)(len & 0xFF);
    }
    return sendAll(s, (char*)hdr, h) && sendAll(s, payload, len);
}

// Read one client->server frame; unmask; return opcode, payload in `out`.
static bool readClientFrame(socket_t s, int& opcode, std::string& out) {
    unsigned char h2[2];
    int got = 0;
    while (got < 2) {
        int r = recv(s, (char*)h2 + got, 2 - got, 0);
        if (r <= 0) return false;
        got += r;
    }
    opcode = h2[0] & 0x0F;
    bool masked = (h2[1] & 0x80) != 0;
    long long len = h2[1] & 0x7F;
    if (len == 126) {
        unsigned char ext[2]; got = 0;
        while (got < 2) { int r = recv(s, (char*)ext + got, 2 - got, 0); if (r <= 0) return false; got += r; }
        len = ((long long)ext[0] << 8) | ext[1];
    } else if (len == 127) {
        return false; // not expected from our client
    }
    unsigned char mask[4] = {};
    if (masked) {
        got = 0;
        while (got < 4) { int r = recv(s, (char*)mask + got, 4 - got, 0); if (r <= 0) return false; got += r; }
    }
    out.resize((size_t)len);
    got = 0;
    while (got < len) {
        int r = recv(s, &out[got], (int)(len - got), 0);
        if (r <= 0) return false;
        got += r;
    }
    if (masked)
        for (long long i = 0; i < len; ++i) out[(size_t)i] = (char)(out[(size_t)i] ^ mask[i & 3]);
    return true;
}

// ------------------------------------------------------------
// Mock Theta Terminal server
// ------------------------------------------------------------
struct MockServer {
    socket_t listenSock = INVALID_SOCK;
    socket_t client     = INVALID_SOCK;
    uint16_t port       = 0;
    std::thread th;

    std::atomic<bool> handshakeOk{false};
    std::atomic<bool> pathOk{false};
    std::atomic<bool> gotPong{false};
    std::vector<std::string> subs;   // text frames received (after server phase 1)
    std::mutex subsMux;

    bool start() {
        listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listenSock == INVALID_SOCK) return false;
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port = 0; // ephemeral
        if (bind(listenSock, (sockaddr*)&a, sizeof(a)) == SOCK_ERR) return false;
        if (listen(listenSock, 1) == SOCK_ERR) return false;
        sockaddr_in bound{};
#ifdef _WIN32
        int blen = sizeof(bound);
#else
        socklen_t blen = sizeof(bound);
#endif
        getsockname(listenSock, (sockaddr*)&bound, &blen);
        port = ntohs(bound.sin_port);
        th = std::thread([this] { run(); });
        return true;
    }

    void run() {
        client = accept(listenSock, nullptr, nullptr);
        if (client == INVALID_SOCK) return;
        setRecvTimeout(client, 5000);

        // --- 1. HTTP handshake ---
        char req[2048] = {};
        int total = 0;
        while (total < (int)sizeof(req) - 1) {
            int r = recv(client, req + total, sizeof(req) - 1 - total, 0);
            if (r <= 0) return;
            total += r;
            if (strstr(req, "\r\n\r\n")) break;
        }
        pathOk.store(strncmp(req, "GET /v1/events HTTP/1.1", 23) == 0);
        const char* resp =
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: dummy\r\n\r\n";
        if (!sendAll(client, resp, (int)strlen(resp))) return;
        handshakeOk.store(true);

        // --- 2. read the two subscribe frames (option + stock) ---
        for (int i = 0; i < 2; ++i) {
            int op = 0; std::string payload;
            if (!readClientFrame(client, op, payload)) return;
            if (op == 0x01) {
                std::lock_guard<std::mutex> lk(subsMux);
                subs.push_back(payload);
            } else { --i; } // skip non-text (shouldn't happen yet)
        }

        // --- 3. send an option quote — real v2 nested layout, single frame ---
        const char* optQuote =
            "{\"header\":{\"status\":\"CONNECTED\",\"type\":\"QUOTE\"},"
            "\"contract\":{\"security_type\":\"OPTION\",\"root\":\"SPY\","
            "\"expiration\":20241220,\"strike\":500000,\"right\":\"C\"},"
            "\"quote\":{\"ms_of_day\":26622025,\"bid_size\":7,\"bid_exchange\":5,"
            "\"bid\":110.2,\"bid_condition\":50,\"ask_size\":7,\"ask_exchange\":5,"
            "\"ask\":110.5,\"ask_condition\":50,\"date\":20231219}}";
        sendTextFrame(client, optQuote, (int)strlen(optQuote));

        // --- 4. send a stock quote SPLIT across two fragments (RFC6455) ---
        const char* stkQuote =
            "{\"header\":{\"status\":\"CONNECTED\",\"type\":\"QUOTE\"},"
            "\"contract\":{\"security_type\":\"STOCK\",\"root\":\"SPY\"},"
            "\"quote\":{\"bid_size\":3,\"bid\":500.01,\"ask\":500.03}}";
        int stkLen  = (int)strlen(stkQuote);
        int firstHalf = stkLen / 2;
        sendTextFrame(client, stkQuote, firstHalf, /*fin=*/false, /*continuation=*/false);
        sendTextFrame(client, stkQuote + firstHalf, stkLen - firstHalf, /*fin=*/true, /*continuation=*/true);

        // --- 5. ping -> expect pong ---
        const unsigned char ping[2] = { 0x89, 0x00 };
        sendAll(client, (char*)ping, 2);

        // --- 6. a non-quote message: must not crash, must not produce a callback ---
        const char* errMsg =
            "{\"header\":{\"status\":\"CONNECTED\",\"type\":\"ERROR\"},"
            "\"message\":\"Subscription rejected: no permission\"}";
        sendTextFrame(client, errMsg, (int)strlen(errMsg));

        // --- 7. read frames until the unwatch arrives (pong may come first) ---
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            int op = 0; std::string payload;
            if (!readClientFrame(client, op, payload)) break;
            if (op == 0x0A) { gotPong.store(true); continue; }
            if (op == 0x01) {
                std::lock_guard<std::mutex> lk(subsMux);
                subs.push_back(payload);
                if (payload.find("\"add\":false") != std::string::npos) break;
            }
        }
    }

    void stop() {
        if (client     != INVALID_SOCK) { plat_shutdown(client);     plat_close_socket(client);     client     = INVALID_SOCK; }
        if (listenSock != INVALID_SOCK) { plat_close_socket(listenSock); listenSock = INVALID_SOCK; }
        if (th.joinable()) th.join();
    }
};

// ------------------------------------------------------------

static bool waitFor(const std::function<bool()>& pred, int ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

int main() {
    plat_net_init();

    MockServer srv;
    if (!srv.start()) { fprintf(stderr, "FAIL: mock server start\n"); return 1; }
    printf("mock server on 127.0.0.1:%u\n", srv.port);

    ThetaDataClient cli;
    cli.setCallback([](const char* sym, double bid, double ask, double last, int vol) {
        std::lock_guard<std::mutex> lk(g_qMux);
        g_quotes.push_back({ sym, bid, ask, last, vol });
    });
    std::vector<std::string> logs;
    std::mutex logMux;
    cli.onLog = [&](const char* m) {
        std::lock_guard<std::mutex> lk(logMux);
        logs.push_back(m);
    };

    CHECK(cli.connect("127.0.0.1", srv.port), "client connect/handshake");
    CHECK(waitFor([&] { return srv.handshakeOk.load(); }, 3000), "server saw handshake");
    CHECK(srv.pathOk.load(), "handshake requested GET /v1/events");

    // Subscribe: one option, one stock
    CHECK(cli.watch("SPY20241220C500000"), "watch(option) send");
    CHECK(cli.watch("SPY"),                "watch(stock) send");

    // Quotes arrive (single frame + fragmented frame)
    CHECK(waitFor([&] {
        std::lock_guard<std::mutex> lk(g_qMux);
        return g_quotes.size() >= 2;
    }, 4000), "received 2 quote callbacks");

    {
        std::lock_guard<std::mutex> lk(g_qMux);
        bool gotOpt = false, gotStk = false;
        for (const auto& q : g_quotes) {
            if (q.sym == "SPY20241220C500000") {
                gotOpt = true;
                CHECK(std::abs(q.bid - 110.2) < 1e-9, "option bid == 110.2");
                CHECK(std::abs(q.ask - 110.5) < 1e-9, "option ask == 110.5");
                CHECK(q.vol == 7,                     "option bid_size == 7");
            } else if (q.sym == "SPY") {
                gotStk = true;
                CHECK(std::abs(q.bid - 500.01) < 1e-9, "stock bid == 500.01 (fragmented frame reassembled)");
                CHECK(std::abs(q.ask - 500.03) < 1e-9, "stock ask == 500.03");
            }
        }
        CHECK(gotOpt, "option quote decoded with correct symbol key");
        CHECK(gotStk, "stock quote decoded (fragmented message)");
        for (const auto& q : g_quotes)
            CHECK(q.sym != "", "no empty-symbol callbacks");
    }

    // Non-quote message surfaced via onLog (ERROR type), not via the quote callback
    CHECK(waitFor([&] {
        std::lock_guard<std::mutex> lk(logMux);
        for (const auto& l : logs)
            if (l.find("ERROR") != std::string::npos) return true;
        return false;
    }, 2000), "non-quote ERROR message surfaced via onLog");

    // Unsubscribe — triggers the server's final read loop (also confirms pong)
    CHECK(cli.unwatch("SPY20241220C500000"), "unwatch send");

    CHECK(waitFor([&] {
        std::lock_guard<std::mutex> lk(srv.subsMux);
        return srv.subs.size() >= 3;
    }, 3000), "server received 3 stream requests");

    {
        std::lock_guard<std::mutex> lk(srv.subsMux);
        bool foundOptSub = false, foundStkSub = false, foundUnsub = false;
        for (const auto& s : srv.subs) {
            if (s.find("\"sec_type\":\"OPTION\"") != std::string::npos &&
                s.find("\"add\":true")            != std::string::npos) {
                foundOptSub = true;
                CHECK(s.find("\"msg_type\":\"STREAM\"")    != std::string::npos, "option sub: msg_type STREAM");
                CHECK(s.find("\"req_type\":\"QUOTE\"")     != std::string::npos, "option sub: req_type QUOTE");
                CHECK(s.find("\"contract\":{")             != std::string::npos, "option sub: nested contract object");
                CHECK(s.find("\"root\":\"SPY\"")           != std::string::npos, "option sub: root SPY");
                CHECK(s.find("\"expiration\":20241220")    != std::string::npos, "option sub: expiration field");
                CHECK(s.find("\"strike\":500000")          != std::string::npos, "option sub: strike in millicents");
                CHECK(s.find("\"right\":\"C\"")            != std::string::npos, "option sub: right C");
            } else if (s.find("\"sec_type\":\"STOCK\"") != std::string::npos &&
                       s.find("\"add\":true")           != std::string::npos) {
                foundStkSub = true;
                CHECK(s.find("\"root\":\"SPY\"") != std::string::npos, "stock sub: root SPY");
            } else if (s.find("\"add\":false") != std::string::npos) {
                foundUnsub = true;
                CHECK(s.find("\"sec_type\":\"OPTION\"") != std::string::npos, "unsub: option contract");
            }
        }
        CHECK(foundOptSub, "option subscribe frame seen");
        CHECK(foundStkSub, "stock subscribe frame seen");
        CHECK(foundUnsub,  "unsubscribe (add:false) frame seen");
    }

    CHECK(waitFor([&] { return srv.gotPong.load(); }, 2000), "client answered ping with pong");

    cli.disconnect();
    srv.stop();

    if (g_failures == 0) {
        printf("ALL TESTS PASSED\n");
        return 0;
    }
    fprintf(stderr, "%d FAILURE(S)\n", g_failures);
    return 1;
}
