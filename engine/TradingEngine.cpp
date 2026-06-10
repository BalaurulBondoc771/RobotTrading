#include "platform.h"
#include "TradingEngine.h"
#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <sstream>
#include <cctype>

// ============================================================
// AUDIT JOURNAL — append-only, daily file, flushed per event.
// Events are rare (order placements, fills, stops), so a mutex +
// fprintf + fflush is fine; it is never called under a spinlock.
// ============================================================

AuditLog::~AuditLog() {
    std::lock_guard<std::mutex> lk(_m);
    if (_f) { fclose(_f); _f = nullptr; }
}

void AuditLog::write(const char* fmt, ...) {
    std::lock_guard<std::mutex> lk(_m);
    time_t t = time(nullptr);
    tm tmv{};
#ifdef _WIN32
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    if (!_f) {
        char name[64];
        snprintf(name, sizeof(name), "audit_%04d%02d%02d.log",
                 tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
        _f = fopen(name, "a");
        if (!_f) return;
    }
    fprintf(_f, "%04d-%02d-%02d %02d:%02d:%02d | ",
            tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
            tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
    va_list va;
    va_start(va, fmt);
    vfprintf(_f, fmt, va);
    va_end(va);
    fputc('\n', _f);
    fflush(_f);
}

// ============================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================

TradingEngine::TradingEngine() {
    _tradeClient = new EClientSocket(this, &_tradeSignal);
    _infoClient  = new EClientSocket(this, &_infoSignal);

    // Pre-reserve all containers to avoid rehash/realloc during trading
    _activeIqSymbols.reserve(4096);
    _processedExecIds.reserve(256);
    _hedgeOrders.reserve(64);
    _positions.reserve(64);
    expirations.reserve(128);
    _chainData.reserve(128);

    _audit.write("ENGINE START");

    _watchRun.store(true);
    _watchThread = std::thread([this] { watchdogLoop(); });
    plat_thread_below_norm(_watchThread.native_handle());
}

TradingEngine::~TradingEngine() {
    // Stop the watchdog first — it touches _tradeClient and _thetaClient.
    _watchRun.store(false);
    if (_watchThread.joinable()) _watchThread.join();
    disconnectIB();
    disconnectIQ();
    delete _tradeReader; delete _tradeClient;
    delete _infoReader;  delete _infoClient;
    _audit.write("ENGINE STOP");
}

// ============================================================
// CONNECTION
// ============================================================

bool TradingEngine::connectIB(const char* host, int tradePort) {
    // Tear down any prior connection first (joins stale reader threads and frees
    // their EReaders) so reconnecting after a drop never reassigns over a
    // joinable std::thread (-> std::terminate) or leaks an EReader.
    disconnectIB();
    // Clock-derived id: rand() without srand() returned the same sequence every
    // run, so two engine instances would collide on the same clientId.
    _clientId = (int)(Clock::now().time_since_epoch().count() % 9000) + 1000;

    logf("IB connecting DUAL-SOCKET (clientId=%d)...", _clientId);

    // Trade socket — highest priority, dedicated to order execution
    if (!_tradeClient->eConnect(host, tradePort, _clientId)) {
        log("IB: trade socket connect FAILED");
        return false;
    }

    // Apply low-latency socket options to the underlying OS socket.
    // EClientSocket exposes the file descriptor via fd().
    {
        int fd = _tradeClient->fd();
        if (fd >= 0) {
            // Disable Nagle — sends each placeOrder() packet immediately instead of
            // waiting up to 40 ms to batch with other small writes.
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one));

            // Small send buffer: keeps the kernel queue short so our writes never
            // block behind stale data — minimises kernel-side queuing latency.
            int sndbuf = 4096;
            setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char*)&sndbuf, sizeof(sndbuf));

            // Large recv buffer so we never drop inbound execution confirmations.
            int rcvbuf = 1 << 20;
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char*)&rcvbuf, sizeof(rcvbuf));

            // Linux: SO_BUSY_POLL + TCP_QUICKACK — reduces execution-report latency.
            plat_socket_latency(fd);
        }
    }

    _tradeReader = new EReader(_tradeClient, &_tradeSignal);
    _tradeReader->start();
    _tradeThread = std::thread([this] {
        while (_tradeClient->isConnected()) {
            _tradeSignal.waitForSignal();
            _tradeReader->processMsgs();
        }
    });
    // Pin the trade reader thread to logical core 2 (leave cores 0-1 for the OS
    // and ThetaData recv thread which is on core 3 — change to suit your CPU topology).
    plat_pin_thread(_tradeThread.native_handle(), 2);
    plat_thread_high(_tradeThread.native_handle());

    // Info socket — stagger by 500 ms so TWS validates the first connection first
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (!_infoClient->eConnect(host, tradePort, _clientId + 1)) {
        log("IB: info socket connect FAILED");
        disconnectIB();  // don't leave a half-connected state (trade socket up, info down)
        return false;
    }
    _infoReader = new EReader(_infoClient, &_infoSignal);
    _infoReader->start();
    _infoThread = std::thread([this] {
        while (_infoClient->isConnected()) {
            _infoSignal.waitForSignal();
            _infoReader->processMsgs();
        }
    });
    plat_thread_above_norm(_infoThread.native_handle());

    // Request account & positions after a brief settle
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    if (_infoClient->isConnected()) {
        SpinLockGuard g(_ibSendLock);
        _infoClient->reqManagedAccts();
        _infoClient->reqPositions();
    }

    log("IB DUAL-SOCKET ACTIVE");
    _audit.write("IB CONNECTED %s:%d clientId=%d", host, tradePort, _clientId);
    return true;
}

void TradingEngine::disconnectIB() {
    // eDisconnect + issueSignal are safe/idempotent even if already down. The
    // reader threads block in waitForSignal(), so we must ALWAYS signal them
    // (not only when isConnected()) or the joins below could hang after a
    // server-side drop. Delete the EReaders only after the threads have stopped
    // so a later connectIB() recreates them without leaking or using a freed one.
    if (_tradeClient) _tradeClient->eDisconnect();
    _tradeSignal.issueSignal();
    if (_infoClient) _infoClient->eDisconnect();
    _infoSignal.issueSignal();
    if (_tradeThread.joinable()) _tradeThread.join();
    if (_infoThread.joinable())  _infoThread.join();
    delete _tradeReader; _tradeReader = nullptr;
    delete _infoReader;  _infoReader  = nullptr;
}

bool TradingEngine::connectIQ(const char* host, uint16_t port) {
    _thetaClient.setCallback([this](const char* sym, double bid, double ask, double last, int vol) {
        processIQData(sym, bid, ask, last, vol);
    });
    _thetaClient.onLog = [this](const char* msg) { log(msg); };
    bool ok = _thetaClient.connect(host, port);
    if (ok) {
        // Remember the endpoint so the watchdog can auto-reconnect after a drop.
        strncpy(_iqHost, host, sizeof(_iqHost) - 1);
        _iqHost[sizeof(_iqHost) - 1] = '\0';
        _iqPort = port;
        _iqShouldBeConnected.store(true);
        log("ThetaData connected");
    } else {
        log("ThetaData connect FAILED — is ThetaTerminal running on port 25520?");
    }
    return ok;
}

void TradingEngine::disconnectIQ() {
    _iqShouldBeConnected.store(false);  // user intent: watchdog must not reconnect
    {
        std::lock_guard<std::mutex> lk(_iqSymMutex);
        for (const auto& s : _activeIqSymbols)
            _thetaClient.unwatch(s.c_str());
        _activeIqSymbols.clear();
    }
    _thetaClient.disconnect();
}

bool TradingEngine::ibConnected() const noexcept {
    return _tradeClient && _tradeClient->isConnected();
}

// ============================================================
// CONTRACT SEARCH
// ============================================================

void TradingEngine::searchContract(const char* symbol) {
    if (!_infoClient->isConnected()) { log("IB info socket offline"); return; }

    strncpy(_stockSymbol, symbol, sizeof(_stockSymbol) - 1);
    _stockSymbol[sizeof(_stockSymbol) - 1] = '\0';
    // Convert to upper case
    for (char* c = _stockSymbol; *c; ++c) *c = (char)toupper(*c);

    { SpinLockGuard g(_chainDataLock); _chainData.clear(); }
    {
        std::lock_guard<std::mutex> lk(chainMutex);
        expirations.clear();
    }

    // Subscribe ThetaData to spot price for the underlying
    if (_thetaClient.isConnected()) {
        _thetaClient.watch(_stockSymbol);
        std::lock_guard<std::mutex> lk(_iqSymMutex);
        _activeIqSymbols.insert(_stockSymbol);
    }

    _searchReqId++;
    Contract req{};
    req.symbol   = _stockSymbol;
    req.secType  = "STK";
    req.exchange = "SMART";
    req.currency = "USD";
    {
        SpinLockGuard g(_ibSendLock);
        _infoClient->reqContractDetails(_searchReqId, req);
    }
    logf("Searching topology for %s ...", _stockSymbol);
}

// ============================================================
// CHAIN LOADING
// ============================================================

void TradingEngine::loadChain(const std::string& expiration, double spotFilterPct) {
    if (!_thetaClient.isConnected()) { log("ThetaData offline — cannot load chain"); return; }

    // Unsubscribe all previous chain symbols
    {
        std::lock_guard<std::mutex> lk(_iqSymMutex);
        for (const auto& s : _activeIqSymbols) {
            if (s != _stockSymbol) _thetaClient.unwatch(s.c_str());
        }
        _activeIqSymbols.clear();
        if (_stockSymbol[0]) _activeIqSymbols.insert(_stockSymbol);
    }

    ChainEntry entry;
    {
        SpinLockGuard g(_chainDataLock);
        auto it = _chainData.find(expiration);
        if (it == _chainData.end()) return;
        entry = it->second;
    }

    double spot = getCurrentSpot();
    std::vector<double> strikes(entry.strikes.begin(), entry.strikes.end());

    if (spotFilterPct > 0.0 && spot > 0.0) {
        double lo = spot * (1.0 - spotFilterPct);
        double hi = spot * (1.0 + spotFilterPct);
        strikes.erase(std::remove_if(strikes.begin(), strikes.end(),
            [lo, hi](double s) { return s < lo || s > hi; }), strikes.end());
    }

    // Build a complete NEW snapshot off to the side, then publish it with a
    // single atomic store (RCU). The recv thread may still hold the old
    // snapshot via shared_ptr — it stays valid until the last reader drops it.
    // This is what makes lock-free row updates in processIQData safe.
    auto snap = std::make_shared<ChainSnapshot>();
    snap->rows.resize(strikes.size());
    snap->map.reserve(strikes.size() * 2 + 8);

    for (int i = 0; i < (int)strikes.size(); ++i) {
        ChainRowData& row = snap->rows[i];
        row.rowIndex = i;
        row.strike   = strikes[i];
        strncpy(row.expiration, expiration.c_str(), sizeof(row.expiration) - 1);

        auto callOsi = buildThetaOptionKey(_stockSymbol, expiration.c_str(), 'C', strikes[i]);
        auto putOsi  = buildThetaOptionKey(_stockSymbol, expiration.c_str(), 'P', strikes[i]);
        strncpy(row.callOsi, callOsi.c_str(), sizeof(row.callOsi) - 1);
        strncpy(row.putOsi,  putOsi.c_str(),  sizeof(row.putOsi)  - 1);

        snap->map[callOsi] = { &row, true  };
        snap->map[putOsi]  = { &row, false };
    }

    std::atomic_store_explicit(&_chainSnap, snap, std::memory_order_release);

    {
        std::lock_guard<std::mutex> lk(_iqSymMutex);
        for (const auto& kv : snap->map)
            _activeIqSymbols.insert(kv.first);
        for (const auto& sym : _activeIqSymbols)
            _thetaClient.watch(sym.c_str());
    }

    ++chainVersion;
    logf("Chain loaded: %s  %d strikes", expiration.c_str(), (int)strikes.size());
}

// ============================================================
// ARMING
// ============================================================

void TradingEngine::armContract(const char* symbol, const char* expiry,
                                 double strike, bool isCall,
                                 const char* hedgeSymbol) {
    char right    = isCall ? 'C' : 'P';
    char oppRight = isCall ? 'P' : 'C';

    // Build IBKR Contract objects (stack-allocated, copied into members)
    Contract opt{};
    opt.symbol   = symbol;
    opt.secType  = "OPT";
    opt.exchange = "SMART";
    opt.currency = "USD";
    opt.lastTradeDateOrContractMonth = expiry;
    opt.strike   = strike;
    opt.right    = (right == 'C') ? "C" : "P";
    opt.multiplier = "100";
    _optionContract = opt;

    opt.right = (oppRight == 'C') ? "C" : "P";
    _oppositeContract = opt;

    Contract hdg{};
    hdg.symbol   = hedgeSymbol ? hedgeSymbol : symbol;
    hdg.secType  = "STK";
    hdg.exchange = "SMART";
    hdg.currency = "USD";
    _hedgeContract = hdg;

    // Pre-compute ThetaData symbol keys so strcmp in hot path is a fixed buffer compare
    auto optOsi = buildThetaOptionKey(symbol, expiry, right,    strike);
    auto oppOsi = buildThetaOptionKey(symbol, expiry, oppRight, strike);
    strncpy(_armedOptSymbol, optOsi.c_str(), sizeof(_armedOptSymbol) - 1);
    strncpy(_armedOppSymbol, oppOsi.c_str(), sizeof(_armedOppSymbol) - 1);

    if (hedgeSymbol) strncpy(_stockSymbol, hedgeSymbol, sizeof(_stockSymbol) - 1);

    // Pre-build entry order template — all constant fields set once here.
    // placeEntryOrder() only needs to write lmtPrice + call placeOrder().
    // This removes 8+ std::string assignments from the hot trading path.
    _entryOrderTemplate             = Order{};
    _entryOrderTemplate.action      = "BUY";
    _entryOrderTemplate.orderType   = "LMT";
    _entryOrderTemplate.tif         = "DAY";
    _entryOrderTemplate.outsideRth  = true;
    _entryOrderTemplate.transmit    = true;

    // Pre-build hedge templates — action fixed at arm time, no string write in _hedgeLock.
    _hedgeCallTemplate             = Order{};
    _hedgeCallTemplate.action      = "SELL";
    _hedgeCallTemplate.orderType   = "LMT";
    _hedgeCallTemplate.outsideRth  = false;
    _hedgeCallTemplate.transmit    = true;

    _hedgePutTemplate              = Order{};
    _hedgePutTemplate.action       = "BUY";
    _hedgePutTemplate.orderType    = "LMT";
    _hedgePutTemplate.outsideRth   = false;
    _hedgePutTemplate.transmit     = true;

    // Make sure ThetaData is watching both option legs
    if (_thetaClient.isConnected()) {
        std::lock_guard<std::mutex> lk(_iqSymMutex);
        if (!_activeIqSymbols.count(optOsi)) {
            _thetaClient.watch(_armedOptSymbol);
            _activeIqSymbols.insert(optOsi);
        }
        if (!_activeIqSymbols.count(oppOsi)) {
            _thetaClient.watch(_armedOppSymbol);
            _activeIqSymbols.insert(oppOsi);
        }
    }

    _isCall       = isCall;
    _armedStrike  = strike;
    // Release store: all preceding writes (_armedStrike, _isCall, _armedOptSymbol, templates...)
    // become visible to any thread that subsequently reads _contractsSet with acquire.
    _contractsSet.store(true, std::memory_order_release);
    logf("ARMED: %s %s %.2f %s | key: %s", symbol, expiry, strike, isCall ? "CALL" : "PUT", _armedOptSymbol);
    _audit.write("ARM %s %s %.2f %s", symbol, expiry, strike, isCall ? "CALL" : "PUT");
}

// ============================================================
// STRATEGY CONTROL
// ============================================================

bool TradingEngine::startStrategy() {
    // Risk gate — checked before anything else so a fat-fingered qty can never start.
    const double oq    = params.optQty.load(std::memory_order_relaxed);
    const double maxOq = params.maxOptQty.load(std::memory_order_relaxed);
    if (oq <= 0 || oq > maxOq) {
        logf("START REFUSED: optQty %.2f outside (0, %.2f] — raise maxOptQty if intended", oq, maxOq);
        return false;
    }

    const char* fail = nullptr;
    DeferredLog dlog;
    bool blindLaunch = false;
    {
        SpinLockGuard guard(_processLock);

        if      (!_contractsSet.load(std::memory_order_relaxed)) fail = "No contract armed";
        else if (!ibConnected())                                 fail = "IB trade socket offline";
        else if (_isStrategyRunning.load())                      fail = "Strategy already running";

        if (!fail) {
            _activeOptQty.store(oq, std::memory_order_relaxed);
            _lastProcessedBid.store(0.0, std::memory_order_relaxed);
            _lastEntryPriceSent = 0;
            _lastEntryModTime   = {};
            _entryOrdersThisRun.store(0);
            _consecRejects.store(0);

            // Stamp qty into the pre-built template now that it is known.
            // Order::totalQuantity is a BID64 Decimal — a raw double assignment
            // would reinterpret the integer as a garbage BID64 encoding.
            _entryOrderTemplate.totalQuantity = DecimalFunctions::doubleToDecimal(oq);

            {
                SpinLockGuard hg(_hedgeLock);
                _sensorExecShares        = 0;
                _sensorOrderStatusFilled = 0;
                _sensorPortfolioDiff     = 0;
                _hedgePlacedOptQty       = 0;
                _hedgeFilledShares       = 0;
                _hedgeRetries            = 0;
                _hedgeAlarmed            = false;
                _entryFillComplete       = false;
                _isHedgeComplete         = false;
                _hedgeOrders.clear();
                _initialPosQty      = _lastKnownOptQty;
                _lastOptionFillTime = {};
                _processedExecIds.clear();   // cleared under _hedgeLock (matches execDetails dedup)
            }

            _activeEntryOrderId.store(0);
            _lastEntryOrderId.store(0);
            _isStrategyRunning.store(true);
            disp.strategyRunning.store(true);

            // For mode 1 (static price), fire immediately
            if (params.entryMode.load(std::memory_order_relaxed) == 1) {
                double sAsk = _stockAsk.load(std::memory_order_relaxed);
                double sBid = _stockBid.load(std::memory_order_relaxed);
                double target = getLegalOptionPrice(params.staticPrice.load(std::memory_order_relaxed));
                const auto now = Clock::now();
                if (sBid > 0 && sAsk > 0) {
                    double mid = (sBid + sAsk) * 0.5;
                    double intrinsic = _isCall ? std::max(0.0, mid - _armedStrike)
                                               : std::max(0.0, _armedStrike - mid);
                    double edge = intrinsic - target;
                    if (edge >= params.dynDiscount.load(std::memory_order_relaxed)) {
                        dlog = placeEntryOrder(target, now);
                    } else {
                        stopStrategyLocked("Static price unsafe at start");
                    }
                } else {
                    // No spot price yet — fire blind at static price (matches C# behaviour)
                    blindLaunch = true;
                    dlog = placeEntryOrder(target, now);
                }
            }
        }
    } // _processLock released — safe to log / audit now

    if (fail) { log(fail); return false; }

    if (_isStrategyRunning.load()) {
        log("STRATEGY STARTED");
        _audit.write("STRATEGY START mode=%d optQty=%.2f static=%.2f disc=%.2f",
                     params.entryMode.load(), oq,
                     params.staticPrice.load(), params.dynDiscount.load());
    }
    if (blindLaunch)  log("M1 blind launch (no spot data yet)");
    if (dlog.pending) emitDeferred(dlog);
    return true;
}

void TradingEngine::stopStrategyLocked(const char* reason) noexcept {
    // Called while _processLock is already held
    if (!_isStrategyRunning.load()) return;
    _isStrategyRunning.store(false);
    disp.strategyRunning.store(false);

    {
        SpinLockGuard hg(_hedgeLock);
        if (_hedgePlacedOptQty > 0)
            _lastKnownOptQty = std::max(_lastKnownOptQty, _initialPosQty + _hedgePlacedOptQty);
        _initialPosQty = _lastKnownOptQty;
    }

    int id = _activeEntryOrderId.exchange(0);
    if (id && _tradeClient && _tradeClient->isConnected()) {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->cancelOrder(id, OrderCancel{});
    }

    logf("AUTO-STOP: %s", reason);

    // Park the audit entry — fprintf must not run under _processLock.
    // The watchdog thread flushes it within ~100 ms.
    snprintf(_stopReason, sizeof(_stopReason), "%s", reason);
    _stopAuditPending.store(true, std::memory_order_release);
}

void TradingEngine::stopStrategy(const char* reason) {
    SpinLockGuard guard(_processLock);
    stopStrategyLocked(reason);
}

void TradingEngine::panicCancelAll() {
    stopStrategy("PANIC");
    if (_tradeClient && _tradeClient->isConnected()) {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->reqGlobalCancel(OrderCancel{});
    }
    log("EMERGENCY STOP TRIGGERED");
    _audit.write("PANIC — global cancel sent");
}

void TradingEngine::closeAllPositionsMKT() {
    stopStrategy("Close All Positions (MKT)");
    if (!_tradeClient || !_tradeClient->isConnected()) {
        log("CLOSE ALL: IB offline");
        return;
    }
    {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->reqGlobalCancel(OrderCancel{});
    }

    // Copy the position map so no lock is held while placing orders.
    std::vector<PosRec> snapshot;
    {
        std::lock_guard<std::mutex> lk(_posMutex);
        snapshot.reserve(_positions.size());
        for (const auto& kv : _positions)
            if (std::abs(kv.second.qty) > 1e-9) snapshot.push_back(kv.second);
    }

    if (snapshot.empty()) {
        log("CLOSE ALL: no open positions known");
        return;
    }

    for (const auto& p : snapshot) {
        Contract c = p.con;
        if (c.exchange.empty()) c.exchange = "SMART";  // position() reports no exchange
        if (c.currency.empty()) c.currency = "USD";

        Order o{};
        o.action        = p.qty > 0 ? "SELL" : "BUY";
        o.orderType     = "MKT";
        o.tif           = "DAY";
        o.totalQuantity = DecimalFunctions::doubleToDecimal(std::abs(p.qty));
        o.transmit      = true;

        int id = getNextOrderId();
        {
            SpinLockGuard sg(_ibSendLock);
            _tradeClient->placeOrder(id, c, o);
        }
        logf("CLOSE %s %.0f %s %s @ MKT (id=%d)", o.action.c_str(),
             std::abs(p.qty), c.symbol.c_str(), c.secType.c_str(), id);
        _audit.write("CLOSE-ALL %s %.2f %s %s strike=%.2f right=%s id=%d",
                     o.action.c_str(), std::abs(p.qty), c.symbol.c_str(),
                     c.secType.c_str(), c.strike, c.right.c_str(), id);
    }
    logf("CLOSE ALL: %d MKT orders sent", (int)snapshot.size());
}

// ============================================================
// HOT PATH: ThetaData quote → price update → order decision
// Called on TIME_CRITICAL ThetaData receive thread (core 3).
// No heap allocation, no blocking, minimal branching.
// ============================================================

void TradingEngine::processIQData(const char* symbol, double bid, double ask,
                                   double last, int vol) noexcept {
    bool triggerProcess = false;

    // 1. Check armed stock/hedge symbol — most common trigger
    if (_stockSymbol[0] != 0 && strcmp(symbol, _stockSymbol) == 0) {
        if (bid > 0) {
            _stockBid.store(bid, std::memory_order_relaxed);
            disp.stockBid.store(bid, std::memory_order_relaxed);
        }
        if (ask > 0) {
            _stockAsk.store(ask, std::memory_order_relaxed);
            disp.stockAsk.store(ask, std::memory_order_relaxed);
        }
        triggerProcess = true;
    }

    // 2. Check armed option symbol
    if (_armedOptSymbol[0] && strcmp(symbol, _armedOptSymbol) == 0) {
        if (bid > 0) {
            _optBid.store(bid, std::memory_order_relaxed);
            disp.optBid.store(bid, std::memory_order_relaxed);
        }
        if (ask > 0) {
            _optAsk.store(ask, std::memory_order_relaxed);
            disp.optAsk.store(ask, std::memory_order_relaxed);
        }
        triggerProcess = true;
    }
    // 3. Check opposite option symbol
    else if (_armedOppSymbol[0] && strcmp(symbol, _armedOppSymbol) == 0) {
        if (bid > 0) {
            _oppOptBid.store(bid, std::memory_order_relaxed);
            disp.oppOptBid.store(bid, std::memory_order_relaxed);
        }
        if (ask > 0) {
            _oppOptAsk.store(ask, std::memory_order_relaxed);
            disp.oppOptAsk.store(ask, std::memory_order_relaxed);
        }
    }

    // 4. Fire price engine if we got new data for a trading-relevant symbol
    if (triggerProcess) {
        // Heartbeat for the staleness watchdog — one relaxed increment (~5 ns),
        // no clock call on the hot path.
        _tickSeq.fetch_add(1, std::memory_order_relaxed);
        processPrice(_stockBid.load(std::memory_order_relaxed),
                     _stockAsk.load(std::memory_order_relaxed));
    }

    // 5. Update chain grid row (UI only). The snapshot shared_ptr is loaded
    // atomically and held for the duration of the access, so a concurrent
    // loadChain() can never free the rows under us (RCU — see ChainSnapshot).
    auto snap = std::atomic_load_explicit(&_chainSnap, std::memory_order_acquire);
    if (!snap) return;
    auto it = snap->map.find(symbol);
    if (it != snap->map.end()) {
        ChainRowData* rd = it->second.rowData;
        if (it->second.isCall) {
            if (bid  >= 0) rd->callBid.store(bid,  std::memory_order_relaxed);
            if (ask  >= 0) rd->callAsk.store(ask,  std::memory_order_relaxed);
            if (last >  0) rd->callLast.store(last, std::memory_order_relaxed);
            if (vol  >= 0) rd->callVol.store(vol,  std::memory_order_relaxed);
            rd->callReceived.store(true, std::memory_order_release);
        } else {
            if (bid  >= 0) rd->putBid.store(bid,  std::memory_order_relaxed);
            if (ask  >= 0) rd->putAsk.store(ask,  std::memory_order_relaxed);
            if (last >  0) rd->putLast.store(last, std::memory_order_relaxed);
            if (vol  >= 0) rd->putVol.store(vol,  std::memory_order_relaxed);
            rd->putReceived.store(true, std::memory_order_release);
        }
    }
}

// ============================================================
// PRICE ENGINE — called from processIQData on TIME_CRITICAL thread.
// Spinlock instead of mutex: section held for < 1 µs normally.
// ============================================================

void TradingEngine::processPrice(double stockBid, double stockAsk) noexcept {
    // Pre-filter 1: no contract armed — acquire sync with armContract's release store ensures
    // _armedStrike, _isCall, _armedOptSymbol and all templates are visible once this is true.
    if (!_contractsSet.load(std::memory_order_acquire)) return;

    const int entryMode = params.entryMode.load(std::memory_order_relaxed);

    // Pre-filter 2 (Mode 2): skip spinlock when spot hasn't moved enough.
    if (entryMode == 2 &&
        std::abs(stockBid - _lastProcessedBid.load(std::memory_order_relaxed))
            < params.spotSens.load(std::memory_order_relaxed)) return;

    // Read clock before acquiring the lock — removes a syscall from the critical section.
    const auto now = Clock::now();
    DeferredLog dlog;
    {
        SpinLockGuard guard(_processLock);

        if (stockAsk <= 0 || stockBid <= 0 || std::isnan(stockAsk) ||
            std::isnan(stockBid) ||
            stockAsk > params.maxSafePrice.load(std::memory_order_relaxed)) return;

        // Advance the Mode-2 pre-filter baseline only after the tick is known
        // valid — a transient bad/over-cap ask must not move _lastProcessedBid,
        // or the spotSens filter would suppress later valid ticks at this bid.
        if (entryMode == 2) _lastProcessedBid.store(stockBid, std::memory_order_relaxed);

        const bool isCall = _isCall;
        const double mid  = (stockBid + stockAsk) * 0.5;
        const double intrinsic = isCall ? std::max(0.0, mid - _armedStrike)
                                        : std::max(0.0, _armedStrike - mid);

        const double dynDiscount = params.dynDiscount.load(std::memory_order_relaxed);
        double targetBid, currentEdge;
        if (entryMode == 1) {
            targetBid    = getLegalOptionPrice(params.staticPrice.load(std::memory_order_relaxed));
            currentEdge  = intrinsic - targetBid;
        } else {
            double raw   = intrinsic - dynDiscount;
            if (raw < 0.05) raw = 0.05;
            targetBid    = getLegalOptionPrice(raw);
            currentEdge  = intrinsic - targetBid;
        }

        disp.targetBid.store(targetBid,   std::memory_order_relaxed);
        disp.currentEdge.store(currentEdge, std::memory_order_relaxed);

        if (!_isStrategyRunning.load(std::memory_order_relaxed)) return;

        const int updateDelayMs = params.updateDelayMs.load(std::memory_order_relaxed);
        if (entryMode == 1) {
            if (currentEdge >= dynDiscount) {
                int activeId = _activeEntryOrderId.load(std::memory_order_relaxed);
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastEntryModTime).count();
                if (activeId == 0) {
                    if (ms > updateDelayMs) dlog = placeEntryOrder(targetBid, now);
                } else if (std::abs(_lastEntryPriceSent - targetBid) > 0.001) {
                    if (ms > updateDelayMs) dlog = updateEntryOrder(targetBid, now);
                }
            } else {
                if (_activeEntryOrderId.load(std::memory_order_relaxed) != 0)
                    stopStrategyLocked("Edge lost (Mode 1)");
            }
        } else {
            // Mode 2: _lastProcessedBid was committed above, after the validity gate
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastEntryModTime).count();
            if (ms > updateDelayMs) {
                int activeId = _activeEntryOrderId.load(std::memory_order_relaxed);
                if (activeId == 0)
                    dlog = placeEntryOrder(targetBid, now);
                else if (std::abs(_lastEntryPriceSent - targetBid) > 0.001)
                    dlog = updateEntryOrder(targetBid, now);
            }
        }
    } // _processLock released — safe to call onLog now
    if (dlog.pending) emitDeferred(dlog);
}

// ============================================================
// ORDER PLACEMENT — called with _processLock held
// Order and Contract objects are stack-allocated (no heap).
// ============================================================

TradingEngine::DeferredLog TradingEngine::placeEntryOrder(double price, TimePoint now) noexcept {
    DeferredLog dlog;
    if (_activeOptQty.load(std::memory_order_relaxed) <= 0) return dlog;

    // Runaway guard: a bug in the update loop must not hammer IBKR forever.
    if (_entryOrdersThisRun.fetch_add(1, std::memory_order_relaxed)
            >= params.maxOrdersPerRun.load(std::memory_order_relaxed)) {
        stopStrategyLocked("Entry order cap reached (maxOrdersPerRun)");
        return dlog;
    }

    const double lp = price; // already getLegalOptionPrice() — computed once in processPrice/startStrategy

    // totalQuantity set once in startStrategy() — only price changes per order.
    _entryOrderTemplate.lmtPrice = lp;

    int newId = getNextOrderId();
    _activeEntryOrderId.store(newId);
    _lastEntryOrderId.store(newId);
    {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->placeOrder(newId, _optionContract, _entryOrderTemplate);
    }
    _lastEntryPriceSent = lp;
    _lastEntryModTime   = now;

    snprintf(dlog.msg, sizeof(dlog.msg), "BUY OPT @ %.2f (id=%d)", lp, newId);
    dlog.pending = true;
    return dlog;
}

TradingEngine::DeferredLog TradingEngine::updateEntryOrder(double price, TimePoint now) noexcept {
    DeferredLog dlog;
    int activeId = _activeEntryOrderId.load(std::memory_order_relaxed);
    if (activeId <= 0) return dlog;

    if (_entryOrdersThisRun.fetch_add(1, std::memory_order_relaxed)
            >= params.maxOrdersPerRun.load(std::memory_order_relaxed)) {
        stopStrategyLocked("Entry order cap reached (maxOrdersPerRun)");
        return dlog;
    }

    const double lp = price; // already getLegalOptionPrice() — computed once in processPrice

    _entryOrderTemplate.lmtPrice = lp;
    {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->placeOrder(activeId, _optionContract, _entryOrderTemplate);
    }
    _lastEntryPriceSent = lp;
    _lastEntryModTime   = now;

    snprintf(dlog.msg, sizeof(dlog.msg), "UPDATE OPT @ %.2f (id=%d)", lp, activeId);
    dlog.pending = true;
    return dlog;
}

// ============================================================
// HEDGE ENGINE — called from IBKR reader thread callbacks
// ============================================================

void TradingEngine::evaluateHedge() noexcept {
    DeferredLog dlog;
    bool targetFilled = false;
    {
        SpinLockGuard guard(_hedgeLock);

        // Retry budget exhausted — a human must intervene; stop auto-placing.
        if (_hedgeAlarmed) return;

        double maxKnown = std::max({_sensorExecShares,
                                    _sensorOrderStatusFilled,
                                    _sensorPortfolioDiff});
        const double activeQty = _activeOptQty.load(std::memory_order_relaxed);
        if (maxKnown > activeQty) maxKnown = activeQty;

        // PLACED is not FILLED: _hedgePlacedOptQty only tracks coverage we have
        // *ordered*. Fill confirmation and shortfall retry live in orderStatus().
        double unplaced = maxKnown - _hedgePlacedOptQty;
        if (unplaced > 0) {
            _hedgePlacedOptQty += unplaced;
            dlog = executeHedgeActual(unplaced);
        }

        if (!_entryFillComplete && maxKnown >= activeQty && activeQty > 0) {
            _entryFillComplete = true;
            _isStrategyRunning.store(false);
            disp.strategyRunning.store(false);
            targetFilled = true;
        }
    } // _hedgeLock released — safe to call onLog now
    if (dlog.pending)  emitDeferred(dlog);
    if (targetFilled) {
        log("TARGET FILLED — auto-stop");
        _audit.write("TARGET FILLED — entry complete, strategy stopped");
    }
}

TradingEngine::DeferredLog TradingEngine::executeHedgeActual(double optShares) noexcept {
    DeferredLog dlog;
    if (!_contractsSet.load(std::memory_order_relaxed)) return dlog;

    double sBid = _stockBid.load(std::memory_order_relaxed);
    double sAsk = _stockAsk.load(std::memory_order_relaxed);
    const double hedgeOffset = params.hedgeOffset.load(std::memory_order_relaxed);

    double limitPrice;
    Order& htmpl = _isCall ? _hedgeCallTemplate : _hedgePutTemplate;
    if (_isCall)
        limitPrice = (sAsk > 0) ? sAsk - hedgeOffset : sBid;
    else
        limitPrice = (sAsk > 0) ? sAsk + hedgeOffset : sBid;
    double safePx = std::round(limitPrice * 100.0) / 100.0;

    double qty = optShares * params.hedgeQty.load(std::memory_order_relaxed);

    htmpl.totalQuantity = DecimalFunctions::doubleToDecimal(qty);
    htmpl.lmtPrice      = safePx;

    int hid = getNextOrderId();
    // Record before sending so an instant orderStatus callback finds the entry.
    HedgeOrderRec rec;
    rec.qtyShares = qty;
    rec.lmtPrice  = safePx;
    rec.open      = true;
    rec.placedAt  = Clock::now();
    _hedgeOrders[hid] = rec;
    {
        SpinLockGuard sg(_ibSendLock);
        _tradeClient->placeOrder(hid, _hedgeContract, htmpl);
    }

    snprintf(dlog.msg, sizeof(dlog.msg), "HEDGE: %s %.0f STK @ %.2f (id=%d)",
             htmpl.action.c_str(), qty, safePx, hid);
    dlog.pending = true;
    return dlog;
}

// ============================================================
// IBKR EWrapper CALLBACKS (meaningful implementations)
// ============================================================

void TradingEngine::nextValidId(int orderId) {
    // Atomic max — a non-atomic load-then-store could race getNextOrderId().
    int cur = _nextOrderId.load(std::memory_order_relaxed);
    while (orderId > cur &&
           !_nextOrderId.compare_exchange_weak(cur, orderId, std::memory_order_relaxed)) {}
}

void TradingEngine::currentTime(long long /*time*/) {
    // Response to the watchdog's 1 Hz reqCurrentTime ping. The previous version
    // re-armed the request from here, which would have self-sustained at RTT
    // period and flooded TWS — the watchdog owns the cadence now.
    long long reqNs = _ibPingReqNs.load(std::memory_order_relaxed);
    if (reqNs > 0) {
        long long nowNs = Clock::now().time_since_epoch().count();
        disp.ibPingMs.store((int)((nowNs - reqNs) / 1000000));
    }
}

void TradingEngine::error(int id, time_t /*errorTime*/, int errorCode,
                           const std::string& errorStr, const std::string& /*json*/) {
    // Suppress known informational codes
    static const int suppress[] = { 321, 2104, 2106, 2107, 2108, 2109, 2158, 200, 162, 354, 10090 };
    for (int c : suppress) if (errorCode == c) return;

    // Suppress by message content (case-insensitive)
    if (!errorStr.empty()) {
        // Build lowercase copy for matching (only when error actually exists)
        std::string lmsg(errorStr.size(), '\0');
        for (size_t i = 0; i < errorStr.size(); ++i)
            lmsg[i] = (char)tolower((unsigned char)errorStr[i]);

        if (lmsg.find("doesn't match report") != std::string::npos ||
            lmsg.find("ignoring")             != std::string::npos ||
            lmsg.find("outside rth")          != std::string::npos ||
            lmsg.find("warning attribute")    != std::string::npos) return;

        // Already-filled rejections are not real errors
        if ((errorCode == 201 || errorCode == 202) &&
            lmsg.find("already filled") != std::string::npos) return;
    }

    logf("ERR %d: %s", errorCode, errorStr.c_str());
    _audit.write("IB ERR %d (id=%d): %s", errorCode, id, errorStr.c_str());

    if (id > 0) {
        bool isFatal = (errorCode == 201 || errorCode == 202 ||
                        errorCode == 104 || errorCode == 203 || errorCode == 399);
        if (isFatal) {
            bool hedgeRejected = false;
            bool hedgeAlarm    = false;
            {
                // Hold _processLock so the compare-and-clear cannot race the order
                // placement path (which stores _activeEntryOrderId under the same
                // lock). Use stopStrategyLocked since we already hold the lock; it
                // also exchanges _activeEntryOrderId to 0 and cancels the order.
                SpinLockGuard guard(_processLock);
                if (id == _activeEntryOrderId.load(std::memory_order_relaxed)) {
                    stopStrategyLocked("IBKR rejected order");
                }
            }
            {
                // Hedge rejection: give the unfilled coverage back so
                // evaluateHedge re-places it, within the retry budget.
                SpinLockGuard hg(_hedgeLock);
                auto it = _hedgeOrders.find(id);
                if (it != _hedgeOrders.end() && it->second.open) {
                    it->second.open = false;
                    double shortfall = it->second.qtyShares - it->second.filledShares;
                    double hq = params.hedgeQty.load(std::memory_order_relaxed);
                    if (shortfall > 1e-9 && hq > 0) {
                        _hedgePlacedOptQty -= shortfall / hq;
                        if (++_hedgeRetries > params.maxHedgeRetries.load(std::memory_order_relaxed)) {
                            _hedgeAlarmed = true;
                            hedgeAlarm    = true;
                        } else {
                            hedgeRejected = true;
                        }
                    }
                }
            }
            if (hedgeRejected) {
                logf("HEDGE order %d rejected — re-placing", id);
                _audit.write("HEDGE REJECTED id=%d — re-placing", id);
                evaluateHedge();
            }
            if (hedgeAlarm) {
                log("!!! HEDGE FAILED REPEATEDLY — POSITION MAY BE NAKED — MANUAL ACTION REQUIRED !!!");
                _audit.write("HEDGE ALARM: retry budget exhausted, id=%d — manual action required", id);
            }

            // Kill switch: repeated fatal rejections mean something is wrong
            // (margin, permissions, contract) — stop hammering the broker.
            int rejects = _consecRejects.fetch_add(1, std::memory_order_relaxed) + 1;
            if (rejects == params.maxConsecRejects.load(std::memory_order_relaxed)) {
                stopStrategy("Kill switch: consecutive rejects");
                if (_tradeClient && _tradeClient->isConnected()) {
                    SpinLockGuard sg(_ibSendLock);
                    _tradeClient->reqGlobalCancel(OrderCancel{});
                }
                log("!!! KILL SWITCH: consecutive order rejects — global cancel sent !!!");
                _audit.write("KILL SWITCH after %d consecutive rejects — global cancel", rejects);
            }
        }
    }
}

void TradingEngine::execDetails(int /*reqId*/, const Contract& contract,
                                 const Execution& exec) {
    // Dedup: the same execution can arrive more than once. _processedExecIds is
    // also cleared by startStrategy under _hedgeLock, so read it under the lock.
    { SpinLockGuard hg(_hedgeLock); if (_processedExecIds.count(exec.execId)) return; }

    bool isOurEntry = (exec.orderId == (long long)_lastEntryOrderId.load() &&
                       _lastEntryOrderId.load() != 0);
    if (!isOurEntry && _isStrategyRunning.load() && _contractsSet.load(std::memory_order_relaxed)) {
        // Match by contract attributes as fallback — must include the expiry, or
        // a same-strike/right fill on a DIFFERENT expiration would be miscounted
        // as our entry and trigger a spurious hedge.
        if (contract.symbol == _optionContract.symbol &&
            std::abs(contract.strike - _optionContract.strike) < 0.001 &&
            contract.right == _optionContract.right &&
            contract.lastTradeDateOrContractMonth == _optionContract.lastTradeDateOrContractMonth &&
            exec.side == "BOT")
            isOurEntry = true;
    }
    bool isOurHedge = false;
    if (!isOurEntry) {
        SpinLockGuard hg(_hedgeLock);
        isOurHedge = _hedgeOrders.count((int)exec.orderId) != 0;
    }

    if (isOurEntry) {
        SpinLockGuard hg(_hedgeLock);
        _processedExecIds.insert(exec.execId);
        _sensorExecShares += DecimalFunctions::decimalToDouble(exec.shares);
        if (_lastOptionFillTime == TimePoint{}) _lastOptionFillTime = Clock::now();
        // Release hedge lock before evaluateHedge (it re-acquires)
    }
    if (isOurEntry) {
        _consecRejects.store(0, std::memory_order_relaxed);  // a real fill ends a reject streak
        _audit.write("FILL ENTRY %s %.2f @ %.4f id=%lld execId=%s",
                     exec.side.c_str(), DecimalFunctions::decimalToDouble(exec.shares),
                     exec.price, (long long)exec.orderId, exec.execId.c_str());
        evaluateHedge();
    }

    if (isOurHedge) {
        double latencyMs = -1.0;
        {
            SpinLockGuard hg(_hedgeLock);
            _processedExecIds.insert(exec.execId);
            if (_lastOptionFillTime != TimePoint{}) {
                latencyMs = std::chrono::duration_cast<std::chrono::microseconds>(
                    Clock::now() - _lastOptionFillTime).count() / 1000.0;
                _lastOptionFillTime = {};
            }
        }
        if (latencyMs >= 0) logf("[LATENCY OPT->STK HEDGE] %.2f ms", latencyMs);
        _consecRejects.store(0, std::memory_order_relaxed);
        _audit.write("FILL HEDGE %s %.2f @ %.4f id=%lld execId=%s",
                     exec.side.c_str(), DecimalFunctions::decimalToDouble(exec.shares),
                     exec.price, (long long)exec.orderId, exec.execId.c_str());
    }
}

void TradingEngine::orderStatus(int orderId, const std::string& status,
                                 Decimal filled, Decimal /*remaining*/,
                                 double /*avgFillPrice*/, long long /*permId*/,
                                 int /*parentId*/, double /*lastFillPrice*/,
                                 int /*clientId*/, const std::string& /*whyHeld*/,
                                 double /*mktCapPrice*/) {
    bool isTerminal = (status == "Cancelled" || status == "Filled" ||
                       status == "Inactive"  || status == "ApiCancelled" ||
                       status == "Rejected");

    if (orderId == _lastEntryOrderId.load() && _lastEntryOrderId.load() != 0) {
        bool doHedge = false;
        double filledD = DecimalFunctions::decimalToDouble(filled);
        {
            SpinLockGuard hg(_hedgeLock);
            if (filledD > _sensorOrderStatusFilled) {
                _sensorOrderStatusFilled = filledD;
                if (_lastOptionFillTime == TimePoint{}) _lastOptionFillTime = Clock::now();
                doHedge = true;
            }
        }
        if (doHedge) evaluateHedge();

        if (isTerminal) {
            _audit.write("ENTRY TERMINAL id=%d status=%s filled=%.2f", orderId, status.c_str(), filledD);
            if (status == "Filled")
                stopStrategy("Entry completely filled");
            else
                stopStrategy(("Entry terminal: " + status).c_str());
        }
        return;
    }

    // ----------------------------------------------------------------
    // Hedge orders: confirm fills (placed != filled), retry shortfalls.
    // ----------------------------------------------------------------
    bool   knownHedge  = false;
    bool   retryHedge  = false;
    bool   alarmHedge  = false;
    bool   hedgeDone   = false;
    double shortfall   = 0.0;
    double filledTotal = 0.0;
    {
        SpinLockGuard hg(_hedgeLock);
        auto it = _hedgeOrders.find(orderId);
        if (it == _hedgeOrders.end()) return;
        knownHedge = true;
        HedgeOrderRec& rec = it->second;

        double filledD = DecimalFunctions::decimalToDouble(filled);
        if (filledD > rec.filledShares) {
            _hedgeFilledShares += filledD - rec.filledShares;
            rec.filledShares = filledD;
        }

        if (isTerminal && rec.open) {
            rec.open  = false;
            shortfall = rec.qtyShares - rec.filledShares;
            if (shortfall > 1e-9 && status != "Filled") {
                // Give the unfilled coverage back so evaluateHedge re-places it.
                double hq = params.hedgeQty.load(std::memory_order_relaxed);
                if (hq > 0) _hedgePlacedOptQty -= shortfall / hq;
                if (++_hedgeRetries > params.maxHedgeRetries.load(std::memory_order_relaxed)) {
                    _hedgeAlarmed = true;
                    alarmHedge    = true;
                } else {
                    retryHedge = true;
                }
            }
        }

        // Hedge truly complete only when every placed share is confirmed filled.
        double targetShares = _hedgePlacedOptQty * params.hedgeQty.load(std::memory_order_relaxed);
        if (!_isHedgeComplete && _entryFillComplete &&
            targetShares > 0 && _hedgeFilledShares + 1e-9 >= targetShares) {
            _isHedgeComplete = true;
            hedgeDone        = true;
        }
        filledTotal = _hedgeFilledShares;
    }

    if (knownHedge && isTerminal) {
        logf("Hedge %d: %s", orderId, status.c_str());
        _audit.write("HEDGE TERMINAL id=%d status=%s", orderId, status.c_str());
    }
    if (retryHedge) {
        logf("HEDGE SHORTFALL %.0f shares (order %d %s) — re-placing", shortfall, orderId, status.c_str());
        _audit.write("HEDGE SHORTFALL %.2f shares id=%d status=%s — re-placing", shortfall, orderId, status.c_str());
        evaluateHedge();
    }
    if (alarmHedge) {
        log("!!! HEDGE FAILED REPEATEDLY — POSITION MAY BE NAKED — MANUAL ACTION REQUIRED !!!");
        _audit.write("HEDGE ALARM: retry budget exhausted, %.2f shares uncovered (id=%d)", shortfall, orderId);
    }
    if (hedgeDone) {
        log("HEDGE COMPLETE — all shares confirmed filled");
        _audit.write("HEDGE COMPLETE — %.2f shares filled", filledTotal);
    }
}

void TradingEngine::contractDetails(int reqId, const ContractDetails& details) {
    if (reqId != _searchReqId) return;
    _currentUndConId = details.contract.conId;
    _lastChainReqId  = _searchReqId + 5000;
    _infoClient->reqSecDefOptParams(
        _lastChainReqId,
        details.contract.symbol, "", "STK",
        _currentUndConId);
}

void TradingEngine::securityDefinitionOptionalParameter(
    int reqId, const std::string& /*exchange*/, int /*underlyingConId*/,
    const std::string& /*tradingClass*/, const std::string& /*multiplier*/,
    const std::set<std::string>& exps, const std::set<double>& strikes) {

    if (reqId != _lastChainReqId) return;
    SpinLockGuard g(_chainDataLock);
    for (const auto& exp : exps) {
        auto& entry = _chainData[exp];
        for (double s : strikes) entry.strikes.insert(s);
    }
}

void TradingEngine::securityDefinitionOptionalParameterEnd(int reqId) {
    if (reqId != _lastChainReqId) return;
    {
        std::lock_guard<std::mutex> lk(chainMutex);
        expirations.clear();
        {
            SpinLockGuard g(_chainDataLock);
            for (const auto& kv : _chainData) expirations.push_back(kv.first);
        }
        std::sort(expirations.begin(), expirations.end());
    }
    ++expVersion;
    logf("Chain topology ready: %d expirations", (int)expirations.size());
}

void TradingEngine::managedAccounts(const std::string& accountsList) {
    std::istringstream ss(accountsList);
    std::string acc;
    if (std::getline(ss, acc, ',') && !acc.empty()) {
        strncpy(mainAccount, acc.c_str(), sizeof(mainAccount) - 1);
        logf("Account: %s", mainAccount);
        if (_infoClient->isConnected()) {
            _infoClient->reqPositions();
            _infoClient->reqAccountUpdates(true, mainAccount);
        }
    }
}

void TradingEngine::position(const std::string& /*account*/,
                              const Contract& contract,
                              Decimal pos, double avgCost) {
    double qty = DecimalFunctions::decimalToDouble(pos);

    // Maintain the full position map — closeAllPositionsMKT iterates it.
    {
        char key[128];
        snprintf(key, sizeof(key), "%s|%s|%s|%.3f|%s",
                 contract.symbol.c_str(), contract.secType.c_str(),
                 contract.lastTradeDateOrContractMonth.c_str(),
                 contract.strike, contract.right.c_str());
        std::lock_guard<std::mutex> lk(_posMutex);
        if (std::abs(qty) < 1e-9) _positions.erase(key);
        else                      _positions[key] = { contract, qty };
    }

    if (!_contractsSet.load(std::memory_order_relaxed)) return;
    if (contract.symbol == _optionContract.symbol &&
        std::abs(contract.strike - _optionContract.strike) < 0.001 &&
        contract.right == _optionContract.right) {
        SpinLockGuard hg(_hedgeLock);
        _lastKnownOptQty = qty;
    }
    (void)avgCost;
}

void TradingEngine::updatePortfolio(const Contract& contract, Decimal position,
                                     double /*marketPrice*/, double /*marketValue*/,
                                     double averageCost, double /*unrealizedPNL*/,
                                     double /*realizedPNL*/,
                                     const std::string& /*accountName*/) {
    if (!_contractsSet.load(std::memory_order_relaxed)) return;
    if (contract.symbol == _optionContract.symbol &&
        std::abs(contract.strike - _optionContract.strike) < 0.001 &&
        contract.right == _optionContract.right &&
        contract.secType == "OPT") {

        bool doHedge = false;
        {
            SpinLockGuard hg(_hedgeLock);
            _lastKnownOptQty = DecimalFunctions::decimalToDouble(position);

            if (_isStrategyRunning.load()) {
                double diff = _lastKnownOptQty - _initialPosQty;
                if (diff > _sensorPortfolioDiff) {
                    _sensorPortfolioDiff = diff;
                    if (_lastOptionFillTime == TimePoint{}) _lastOptionFillTime = Clock::now();
                    doHedge = true;
                }
            }
        }
        if (doHedge) evaluateHedge();
    }
    (void)averageCost;
}

// ============================================================
// HELPERS
// ============================================================

double TradingEngine::getLegalOptionPrice(double price) const noexcept {
    // tickMode 1: penny increments at all prices — penny-interval-program
    // classes (SPY, QQQ, ...) quote in pennies even above $3; rounding to
    // nickels there gives away up to 4 cents of edge per order.
    if (price < 3.00 || params.tickMode.load(std::memory_order_relaxed) == 1)
        return std::round(price * 100.0) / 100.0;
    // Non-penny classes above $3.00 tick in $0.05 increments
    return std::round(price * 20.0) / 20.0;
}

double TradingEngine::getCurrentSpot() const noexcept {
    double bid = _stockBid.load(std::memory_order_relaxed);
    double ask = _stockAsk.load(std::memory_order_relaxed);
    if (bid > 0 && ask > 0) return (bid + ask) * 0.5;
    if (bid > 0)             return bid;
    return 0.0;
}

// Build ThetaData option symbol key from IBKR components.
// Format: "{root}{YYYYMMDD}{C|P}{strike_millicents}"
// e.g. "SPY20241220C500000" for $500 call expiring 2024-12-20.
// Must match the key that ThetaDataClient::processMessage reconstructs from responses.
std::string TradingEngine::buildThetaOptionKey(
    const char* sym, const char* ibkrExpiry, char right, double strike) const {

    if (!ibkrExpiry || strlen(ibkrExpiry) != 8) return "";
    // ibkrExpiry is already YYYYMMDD from IBKR — use it directly.
    int strikeMC = (int)std::round(strike * 1000.0); // dollars → millicents
    char buf[64];
    snprintf(buf, sizeof(buf), "%s%s%c%d", sym, ibkrExpiry, right, strikeMC);
    return buf;
}

int TradingEngine::getNextOrderId() noexcept {
    return _nextOrderId.fetch_add(1, std::memory_order_relaxed);
}

void TradingEngine::log(const char* msg) const {
    if (onLog) onLog(msg);
}

void TradingEngine::logf(const char* fmt, ...) const {
    if (!onLog) return;
    char buf[512];
    va_list va;
    va_start(va, fmt);
    vsnprintf(buf, sizeof(buf), fmt, va);
    va_end(va);
    onLog(buf);
}

void TradingEngine::emitDeferred(const DeferredLog& d) {
    if (!d.pending) return;
    log(d.msg);
    _audit.write("%s", d.msg);
}

// ============================================================
// WATCHDOG — below-normal-priority thread, 100 ms tick.
//   1. Data staleness: strategy running + no relevant tick for staleMs
//      → cancel entry and stop (a resting order with frozen prices is
//      pure adverse selection once the market moves).
//   2. ThetaData auto-reconnect + re-subscribe after a feed drop.
//   3. Hedge chase: reprice unfilled hedge orders, escalate to MKT.
//   4. IB ping initiation (1 Hz; the response lands in currentTime()).
// ============================================================

void TradingEngine::watchdogLoop() noexcept {
    uint64_t lastSeq       = 0;
    auto     lastSeqChange = Clock::now();
    auto     lastPing      = TimePoint{};
    auto     lastReconnect = TimePoint{};

    while (_watchRun.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const auto now = Clock::now();

        // 0) Flush the stop-audit entry parked by stopStrategyLocked
        //    (fprintf must not run while a spinlock is held).
        if (_stopAuditPending.exchange(false, std::memory_order_acquire))
            _audit.write("STRATEGY STOP: %s", _stopReason);

        // 1) Data staleness
        uint64_t seq = _tickSeq.load(std::memory_order_relaxed);
        if (seq != lastSeq) {
            lastSeq = seq;
            lastSeqChange = now;
        } else if (_isStrategyRunning.load(std::memory_order_relaxed)) {
            int staleMs = params.staleMs.load(std::memory_order_relaxed);
            if (staleMs > 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(now - lastSeqChange).count() >= staleMs) {
                stopStrategy("Market data stale — entry cancelled");
                lastSeqChange = now;  // don't re-trigger every 100 ms
            }
        }

        // 1b) Outright feed loss while running — don't wait out staleMs.
        if (_isStrategyRunning.load(std::memory_order_relaxed) &&
            _iqShouldBeConnected.load() && !_thetaClient.isConnected()) {
            stopStrategy("ThetaData feed lost — entry cancelled");
        }

        // 2) ThetaData auto-reconnect (1 Hz attempts)
        if (_iqShouldBeConnected.load() && !_thetaClient.isConnected() &&
            (lastReconnect == TimePoint{} || now - lastReconnect >= std::chrono::seconds(1))) {
            lastReconnect = now;
            if (_thetaClient.connect(_iqHost, _iqPort)) {
                int n = 0;
                {
                    std::lock_guard<std::mutex> lk(_iqSymMutex);
                    for (const auto& s : _activeIqSymbols) _thetaClient.watch(s.c_str());
                    n = (int)_activeIqSymbols.size();
                }
                logf("ThetaData reconnected — %d subscriptions restored", n);
                _audit.write("THETA RECONNECTED (%d symbols)", n);
            }
        }

        // 3) Hedge chase
        chaseHedges(now);

        // 4) IB ping
        if ((lastPing == TimePoint{} || now - lastPing >= std::chrono::seconds(1)) && ibConnected()) {
            lastPing = now;
            _ibPingReqNs.store(now.time_since_epoch().count(), std::memory_order_relaxed);
            SpinLockGuard sg(_ibSendLock);
            _tradeClient->reqCurrentTime();
        }
    }
}

void TradingEngine::chaseHedges(TimePoint now) noexcept {
    const int chaseMs   = params.hedgeChaseMs.load(std::memory_order_relaxed);
    const int maxChases = params.maxHedgeChases.load(std::memory_order_relaxed);
    if (chaseMs <= 0) return;  // 0 disables chasing

    double sBid = _stockBid.load(std::memory_order_relaxed);
    double sAsk = _stockAsk.load(std::memory_order_relaxed);
    const double off = params.hedgeOffset.load(std::memory_order_relaxed);

    // Collect actions under the lock, send after — placeOrder never runs
    // while _hedgeLock is held by this thread.
    struct Chase { int id; double qty; double px; bool mkt; };
    Chase actions[8];
    int nActions = 0;
    {
        SpinLockGuard hg(_hedgeLock);
        for (auto& kv : _hedgeOrders) {
            HedgeOrderRec& rec = kv.second;
            if (!rec.open) continue;
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - rec.placedAt).count() < chaseMs)
                continue;
            if (nActions >= 8) break;

            Chase a{};
            a.id  = kv.first;
            a.qty = rec.qtyShares;
            if (rec.chases >= maxChases) {
                a.mkt = true;
                a.px  = 0;
            } else {
                // Cross the spread progressively: SELL hedges step below the
                // bid, BUY hedges step above the ask, one offset per chase.
                a.mkt = false;
                int step = rec.chases + 1;
                if (_isCall) a.px = (sBid > 0 ? sBid : rec.lmtPrice) - off * step;
                else         a.px = (sAsk > 0 ? sAsk : rec.lmtPrice) + off * step;
                a.px = std::round(a.px * 100.0) / 100.0;
                if (a.px < 0.01) a.px = 0.01;
                rec.lmtPrice = a.px;
            }
            rec.chases++;
            rec.placedAt = now;
            actions[nActions++] = a;
        }
    }

    for (int i = 0; i < nActions; ++i) {
        const Chase& a = actions[i];
        if (!_tradeClient || !_tradeClient->isConnected()) break;
        // Modify in place: same order id, more aggressive price (or MKT).
        Order o = _isCall ? _hedgeCallTemplate : _hedgePutTemplate;
        o.totalQuantity = DecimalFunctions::doubleToDecimal(a.qty);
        if (a.mkt) { o.orderType = "MKT"; o.lmtPrice = 0;    }
        else       {                      o.lmtPrice = a.px; }
        {
            SpinLockGuard sg(_ibSendLock);
            _tradeClient->placeOrder(a.id, _hedgeContract, o);
        }
        if (a.mkt) {
            logf("HEDGE CHASE id=%d -> MKT (unfilled after %d limit chases)", a.id, maxChases);
            _audit.write("HEDGE CHASE id=%d -> MKT", a.id);
        } else {
            logf("HEDGE CHASE id=%d -> %.2f", a.id, a.px);
            _audit.write("HEDGE CHASE id=%d -> %.2f", a.id, a.px);
        }
    }
}
