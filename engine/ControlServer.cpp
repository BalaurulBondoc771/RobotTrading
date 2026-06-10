#include "platform.h"
#include "ControlServer.h"
#include "TradingEngine.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <sstream>

// ============================================================
// Minimal JSON helpers (identical pattern to ThetaDataClient)
// ============================================================

static const char* jFindVal(const char* j, int n, const char* key) noexcept {
    int klen = (int)strlen(key);
    for (int i = 0; i + klen + 2 < n; ++i) {
        if (j[i] == '"' && memcmp(j+i+1, key, klen)==0 && j[i+1+klen]=='"') {
            int k = i + klen + 2;
            while (k < n && j[k]==' ') ++k;          // skip spaces before the colon
            if (k < n && j[k]==':') {                // require a real "key": — not a value that looks like a key
                ++k;
                while (k < n && j[k]==' ') ++k;
                if (k < n) return j + k;
            }
        }
    }
    return nullptr;
}

static int jStr(const char* j, int n, const char* key, char* out, int sz) noexcept {
    const char* v = jFindVal(j, n, key);
    if (!v) { out[0]=0; return 0; }
    if (*v == '"') {
        ++v; int i = 0;
        while (*v && *v != '"' && i < sz-1) out[i++] = *v++;
        out[i] = 0; return i;
    }
    // unquoted (numbers/booleans)
    int i = 0;
    while (*v && *v != ',' && *v != '}' && *v != ']' && i < sz-1) out[i++] = *v++;
    out[i] = 0; return i;
}

static double jDbl(const char* j, int n, const char* key) noexcept {
    const char* v = jFindVal(j, n, key);
    return v ? atof(v) : 0.0;
}

// JSON-escape a string (no heap if string fits in provided buf)
static std::string jsonEsc(const char* s) {
    std::string r;
    r.reserve(strlen(s) + 8);
    for (; *s; ++s) {
        if      (*s == '"')  r += "\\\"";
        else if (*s == '\\') r += "\\\\";
        else if (*s == '\n') r += "\\n";
        else if (*s == '\r') r += "\\r";
        else                 r += *s;
    }
    return r;
}

// ============================================================

ControlServer::ControlServer(TradingEngine& eng, uint16_t port)
    : _eng(eng), _port(port) {}

ControlServer::~ControlServer() { stop(); }

bool ControlServer::start() {
    plat_net_init();

    _listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_listenSock == INVALID_SOCK) return false;

    int one = 1;
    setsockopt(_listenSock, SOL_SOCKET, SO_REUSEADDR, (char*)&one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); // localhost only
    if (bind(_listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR ||
        listen(_listenSock, 8) == SOCK_ERR) {
        plat_close_socket(_listenSock);
        _listenSock = INVALID_SOCK;
        return false;
    }

    _running.store(true);
    _acceptThread = std::thread([this]{ acceptLoop(); });
    _statusThread = std::thread([this]{ statusLoop(); });
    plat_thread_below_norm(_acceptThread.native_handle());
    plat_thread_below_norm(_statusThread.native_handle());
    return true;
}

void ControlServer::stop() {
    if (!_running.exchange(false)) return;
    if (_listenSock != INVALID_SOCK) {
        plat_close_socket(_listenSock);
        _listenSock = INVALID_SOCK;
    }
    {
        std::lock_guard<std::mutex> lk(_clientsMux);
        for (socket_t s : _clients) plat_close_socket(s);
        _clients.clear();
    }
    if (_acceptThread.joinable()) _acceptThread.join();
    if (_statusThread.joinable()) _statusThread.join();
}

void ControlServer::acceptLoop() {
    while (_running.load()) {
        socket_t c = accept(_listenSock, nullptr, nullptr);
        if (c == INVALID_SOCK) break;
        {
            std::lock_guard<std::mutex> lk(_clientsMux);
            _clients.push_back(c);
        }
        std::thread t([this, c]{ clientLoop(c); });
        plat_thread_below_norm(t.native_handle());
        t.detach();
    }
}

void ControlServer::clientLoop(socket_t s) {
    char buf[4096];
    char line[4096];
    int  lineLen = 0;
    bool lineOverflow = false;   // set once a line exceeds the buffer -> drop it

    while (_running.load()) {
        int r = recv(s, buf, sizeof(buf)-1, 0);
        if (r <= 0) break;
        for (int i = 0; i < r; ++i) {
            if (buf[i] == '\n') {
                if (!lineOverflow && lineLen > 2) {
                    line[lineLen] = '\0';
                    handleCommand(line, lineLen);
                }
                lineLen = 0;
                lineOverflow = false;
            } else if (lineLen < (int)sizeof(line)-1) {
                line[lineLen++] = buf[i];
            } else {
                lineOverflow = true; // oversized line: discard rather than run a truncated command
            }
        }
    }

    // Remove from the list and close under the mutex, but only if stop() hasn't
    // already done it — guards against erase(end()) UB and a double-close of s.
    std::lock_guard<std::mutex> lk(_clientsMux);
    auto it = std::find(_clients.begin(), _clients.end(), s);
    if (it != _clients.end()) {
        _clients.erase(it);
        plat_close_socket(s);
    }
}

// ============================================================
// 200 ms status + chain/exp broadcast
// ============================================================

void ControlServer::statusLoop() {
    int tick = 0;
    while (_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        broadcastStatus();

        // Expirations: push when version changes
        int ev = _eng.expVersion.load();
        if (ev != _lastExpVersion) {
            _lastExpVersion = ev;
            broadcastExpirations();
        }

        // Chain: push when version changes (after loadChain)
        int cv = _eng.chainVersion.load();
        if (cv != _lastChainVersion) {
            _lastChainVersion = cv;
            broadcastChain();
        }
        ++tick;
    }
}

// ============================================================
// Broadcast helpers
// ============================================================

void ControlServer::broadcast(const char* json) {
    // Caller must hold _clientsMux — or use broadcastLog (which locks internally)
    for (socket_t s : _clients)
        send(s, json, (int)strlen(json), 0);
}

void ControlServer::broadcastLog(const char* msg, bool isErr) {
    std::string escaped = jsonEsc(msg);
    char buf[2048];
    snprintf(buf, sizeof(buf), "{\"type\":\"log\",\"msg\":\"%s\"%s}\n",
             escaped.c_str(), isErr ? ",\"err\":true" : "");

    std::lock_guard<std::mutex> lk(_clientsMux);
    broadcast(buf);
}

void ControlServer::broadcastStatus() {
    auto& d = _eng.disp;
    char buf[512];
    snprintf(buf, sizeof(buf),
        "{\"type\":\"status\","
        "\"ibConnected\":%s,"
        "\"tdConnected\":%s,"
        "\"stratRunning\":%s,"
        "\"stockBid\":%.4f,"
        "\"stockAsk\":%.4f,"
        "\"optBid\":%.4f,"
        "\"optAsk\":%.4f,"
        "\"targetBid\":%.4f,"
        "\"edge\":%.4f,"
        "\"ibPingMs\":%d,"
        "\"account\":\"%s\"}\n",
        _eng.ibConnected() ? "true" : "false",
        _eng.iqConnected() ? "true" : "false",
        d.strategyRunning.load() ? "true" : "false",
        d.stockBid.load(), d.stockAsk.load(),
        d.optBid.load(),   d.optAsk.load(),
        d.targetBid.load(), d.currentEdge.load(),
        d.ibPingMs.load(),
        _eng.mainAccount
    );
    std::lock_guard<std::mutex> lk(_clientsMux);
    broadcast(buf);
}

void ControlServer::broadcastExpirations() {
    std::string json = "{\"type\":\"exps\",\"list\":[";
    std::lock_guard<std::mutex> chainLk(_eng.chainMutex);
    for (size_t i = 0; i < _eng.expirations.size(); ++i) {
        if (i) json += ',';
        json += '"'; json += _eng.expirations[i]; json += '"';
    }
    json += "]}\n";
    std::lock_guard<std::mutex> lk(_clientsMux);
    broadcast(json.c_str());
}

void ControlServer::broadcastChain() {
    std::string json;
    json.reserve(8192);
    json = "{\"type\":\"chain\",\"rows\":[";

    // RCU snapshot — holding the shared_ptr keeps the rows alive even if the
    // engine publishes a new chain while we serialize this one.
    auto snap = _eng.chainSnapshot();
    if (snap) {
        for (size_t i = 0; i < snap->rows.size(); ++i) {
            const ChainRowData& r = snap->rows[i];
            if (i) json += ',';
            char row[256];
            snprintf(row, sizeof(row),
                "{\"strike\":%.4f,"
                "\"callBid\":%.4f,\"callAsk\":%.4f,"
                "\"putBid\":%.4f,\"putAsk\":%.4f}",
                r.strike,
                r.callBid.load(), r.callAsk.load(),
                r.putBid.load(),  r.putAsk.load());
            json += row;
        }
    }
    json += "]}\n";

    std::lock_guard<std::mutex> lk(_clientsMux);
    broadcast(json.c_str());
}

// ============================================================
// Command dispatch
// ============================================================

void ControlServer::handleCommand(const char* json, int len) {
    char cmd[32]; jStr(json, len, "cmd", cmd, sizeof(cmd));

    if (strcmp(cmd, "cib") == 0) {
        char host[64] = "127.0.0.1";
        char portStr[16] = "4002";
        jStr(json, len, "host", host, sizeof(host));
        jStr(json, len, "port", portStr, sizeof(portStr));
        _eng.connectIB(host, atoi(portStr));

    } else if (strcmp(cmd, "dib") == 0) {
        _eng.disconnectIB();

    } else if (strcmp(cmd, "ciq") == 0) {
        char host[64] = "127.0.0.1";
        char portStr[16] = "25520";
        jStr(json, len, "host", host, sizeof(host));
        jStr(json, len, "port", portStr, sizeof(portStr));
        _eng.connectIQ(host, (uint16_t)atoi(portStr));

    } else if (strcmp(cmd, "diq") == 0) {
        _eng.disconnectIQ();

    } else if (strcmp(cmd, "search") == 0) {
        char sym[32]; jStr(json, len, "symbol", sym, sizeof(sym));
        if (sym[0]) _eng.searchContract(sym);

    } else if (strcmp(cmd, "loadchain") == 0) {
        char exp[16]; jStr(json, len, "exp", exp, sizeof(exp));
        double pct = jDbl(json, len, "pct");
        if (pct <= 0.0) pct = 0.15;
        if (exp[0]) _eng.loadChain(exp, pct);

    } else if (strcmp(cmd, "arm") == 0) {
        char sym[32], exp[16], right[4];
        jStr(json, len, "symbol", sym,   sizeof(sym));
        jStr(json, len, "exp",    exp,   sizeof(exp));
        jStr(json, len, "right",  right, sizeof(right));
        double strike = jDbl(json, len, "strike");
        if (sym[0] && exp[0] && strike > 0)
            _eng.armContract(sym, exp, strike, (right[0]=='C'||right[0]=='c'));

    } else if (strcmp(cmd, "start") == 0) {
        _eng.startStrategy();

    } else if (strcmp(cmd, "stop") == 0) {
        _eng.stopStrategy("Web UI");

    } else if (strcmp(cmd, "panic") == 0) {
        _eng.panicCancelAll();

    } else if (strcmp(cmd, "closeall") == 0) {
        _eng.closeAllPositionsMKT();

    } else if (strcmp(cmd, "setparam") == 0) {
        char key[32]; jStr(json, len, "key", key, sizeof(key));
        double value = jDbl(json, len, "value");
        applyParam(key, value);

    } else if (strcmp(cmd, "quit") == 0) {
        _eng.stopStrategy("Shutdown");
        _eng.disconnectIQ();
        _eng.disconnectIB();
        _running.store(false);
        // Allow main to detect this via isRunning()
    }
}

void ControlServer::applyParam(const char* key, double value) {
    auto& p = _eng.params;

    // Structural params can't change mid-run: entryMode flips would switch the
    // decision logic under a live order, and optQty/hedgeQty are stamped into
    // templates and hedge accounting at start.
    bool running = _eng.disp.strategyRunning.load();
    bool structural = strcmp(key, "entryMode") == 0 ||
                      strcmp(key, "optQty")    == 0 ||
                      strcmp(key, "hedgeQty")  == 0;
    if (running && structural) {
        char buf[96];
        snprintf(buf, sizeof(buf), "REFUSED: %s cannot change while strategy is running", key);
        broadcastLog(buf, true);
        return;
    }

    if      (strcmp(key, "entryMode")       == 0) p.entryMode       = (int)value;
    else if (strcmp(key, "staticPrice")     == 0) p.staticPrice     = value;
    else if (strcmp(key, "dynDiscount")     == 0) p.dynDiscount     = value;
    else if (strcmp(key, "optQty")          == 0) p.optQty          = value;
    else if (strcmp(key, "hedgeQty")        == 0) p.hedgeQty        = value;
    else if (strcmp(key, "updateDelayMs")   == 0) p.updateDelayMs   = (int)value;
    else if (strcmp(key, "spotSens")        == 0) p.spotSens        = value;
    else if (strcmp(key, "hedgeOffset")     == 0) p.hedgeOffset     = value;
    else if (strcmp(key, "maxSafePrice")    == 0) p.maxSafePrice    = value;
    else if (strcmp(key, "tickMode")        == 0) p.tickMode        = (int)value;
    else if (strcmp(key, "staleMs")         == 0) p.staleMs         = (int)value;
    else if (strcmp(key, "hedgeChaseMs")    == 0) p.hedgeChaseMs    = (int)value;
    else if (strcmp(key, "maxHedgeChases")  == 0) p.maxHedgeChases  = (int)value;
    else if (strcmp(key, "maxHedgeRetries") == 0) p.maxHedgeRetries = (int)value;
    else if (strcmp(key, "maxOptQty")       == 0) p.maxOptQty       = value;
    else if (strcmp(key, "maxOrdersPerRun") == 0) p.maxOrdersPerRun = (int)value;
    else if (strcmp(key, "maxConsecRejects")== 0) p.maxConsecRejects= (int)value;
}
