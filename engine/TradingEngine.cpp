#include "platform.h"
#include "TradingEngine.h"
#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <sstream>
#include <cctype>

// ============================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================

TradingEngine::TradingEngine() {
    _tradeClient = new EClientSocket(this, &_tradeSignal);
    _infoClient  = new EClientSocket(this, &_infoSignal);

    // Pre-reserve all containers to avoid rehash/realloc during trading
    _iqChainMap.reserve(4096);
    _activeIqSymbols.reserve(4096);
    _processedExecIds.reserve(256);
    chainRows.reserve(2048);
    expirations.reserve(128);
    _chainData.reserve(128);
}

TradingEngine::~TradingEngine() {
    disconnectIB();
    disconnectIQ();
    delete _tradeReader; delete _tradeClient;
    delete _infoReader;  delete _infoClient;
}

// ============================================================
// CONNECTION
// ============================================================

bool TradingEngine::connectIB(const char* host, int tradePort) {
    _clientId = rand() % 9000 + 1000;

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
        _infoClient->reqManagedAccts();
        _infoClient->reqPositions();
    }

    log("IB DUAL-SOCKET ACTIVE");
    return true;
}

void TradingEngine::disconnectIB() {
    if (_tradeClient && _tradeClient->isConnected()) {
        _tradeClient->eDisconnect();
        _tradeSignal.issueSignal();
    }
    if (_infoClient && _infoClient->isConnected()) {
        _infoClient->eDisconnect();
        _infoSignal.issueSignal();
    }
    if (_tradeThread.joinable()) _tradeThread.join();
    if (_infoThread.joinable())  _infoThread.join();
}

bool TradingEngine::connectIQ(const char* host, uint16_t port) {
    _thetaClient.setCallback([this](const char* sym, double bid, double ask, double last, int vol) {
        processIQData(sym, bid, ask, last, vol);
    });
    bool ok = _thetaClient.connect(host, port);
    if (ok) log("ThetaData connected");
    else     log("ThetaData connect FAILED — is ThetaTerminal running on port 25520?");
    return ok;
}

void TradingEngine::disconnectIQ() {
    for (const auto& s : _activeIqSymbols)
        _thetaClient.unwatch(s.c_str());
    _activeIqSymbols.clear();
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
    expirations.clear();

    // Subscribe ThetaData to spot price for the underlying
    if (_thetaClient.isConnected()) {
        _thetaClient.watch(_stockSymbol);
        _activeIqSymbols.insert(_stockSymbol);
    }

    _searchReqId++;
    Contract req{};
    req.symbol   = _stockSymbol;
    req.secType  = "STK";
    req.exchange = "SMART";
    req.currency = "USD";
    _infoClient->reqContractDetails(_searchReqId, req);
    logf("Searching topology for %s ...", _stockSymbol);
}

// ============================================================
// CHAIN LOADING
// ============================================================

void TradingEngine::loadChain(const std::string& expiration, double spotFilterPct) {
    if (!_thetaClient.isConnected()) { log("ThetaData offline — cannot load chain"); return; }

    // Unsubscribe all previous chain symbols
    for (const auto& s : _activeIqSymbols) {
        if (s != _stockSymbol) _thetaClient.unwatch(s.c_str());
    }
    _activeIqSymbols.clear();
    if (_stockSymbol[0]) _activeIqSymbols.insert(_stockSymbol);

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

    {
        // Take chainMutex before rebuilding chainRows — ControlServer reads this vector.
        std::lock_guard<std::mutex> lk(chainMutex);
        _iqChainMap.clear();
        chainRows.clear();
        chainRows.resize(strikes.size());

        for (int i = 0; i < (int)strikes.size(); ++i) {
            ChainRowData& row = chainRows[i];
            row.rowIndex = i;
            row.strike   = strikes[i];
            strncpy(row.expiration, expiration.c_str(), sizeof(row.expiration) - 1);

            auto callOsi = buildThetaOptionKey(_stockSymbol, expiration.c_str(), 'C', strikes[i]);
            auto putOsi  = buildThetaOptionKey(_stockSymbol, expiration.c_str(), 'P', strikes[i]);
            strncpy(row.callOsi, callOsi.c_str(), sizeof(row.callOsi) - 1);
            strncpy(row.putOsi,  putOsi.c_str(),  sizeof(row.putOsi)  - 1);

            _iqChainMap[callOsi] = { &row, true  };
            _iqChainMap[putOsi]  = { &row, false };
            _activeIqSymbols.insert(callOsi);
            _activeIqSymbols.insert(putOsi);
        }
    }

    for (const auto& sym : _activeIqSymbols)
        _thetaClient.watch(sym.c_str());

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
}

// ============================================================
// STRATEGY CONTROL
// ============================================================

bool TradingEngine::startStrategy() {
    SpinLockGuard guard(_processLock);

    if (!_contractsSet.load(std::memory_order_relaxed)) { log("No contract armed"); return false; }
    if (!ibConnected())            { log("IB trade socket offline");    return false; }
    if (_isStrategyRunning.load()) { log("Strategy already running");   return false; }

    _activeOptQty             = params.optQty;
    _sensorExecShares         = 0;
    _sensorOrderStatusFilled  = 0;
    _sensorPortfolioDiff      = 0;
    _totalHedgedForEntry      = 0;
    _isHedgeComplete          = false;
    _lastProcessedBid         = 0;
    _lastEntryPriceSent       = 0;
    _lastEntryModTime         = {};
    _processedExecIds.clear();

    // Stamp qty into the pre-built templates now that it is known.
    _entryOrderTemplate.totalQuantity = _activeOptQty;

    {
        SpinLockGuard hg(_hedgeLock);
        _initialPosQty   = _lastKnownOptQty;
        _lastOptionFillTime = {};
    }

    _activeEntryOrderId.store(0);
    _lastEntryOrderId.store(0);
    _isStrategyRunning.store(true);
    disp.strategyRunning.store(true);

    log("STRATEGY STARTED");

    // For mode 1 (static price), fire immediately
    if (params.entryMode == 1) {
        double sAsk = _stockAsk.load(std::memory_order_relaxed);
        double sBid = _stockBid.load(std::memory_order_relaxed);
        double target = getLegalOptionPrice(params.staticPrice);
        const auto now = Clock::now();
        if (sBid > 0 && sAsk > 0) {
            double mid = (sBid + sAsk) * 0.5;
            double intrinsic = _isCall ? std::max(0.0, mid - _optionContract.strike)
                                       : std::max(0.0, _optionContract.strike - mid);
            double edge = intrinsic - target;
            if (edge >= params.dynDiscount) {
                placeEntryOrder(target, now);
            } else {
                stopStrategyLocked("Static price unsafe at start");
            }
        } else {
            // No spot price yet — fire blind at static price (matches C# behaviour)
            logf("M1 blind launch @ %.2f (no spot data yet)", target);
            placeEntryOrder(target, now);
        }
    }

    return true;
}

void TradingEngine::stopStrategyLocked(const char* reason) noexcept {
    // Called while _processLock is already held
    if (!_isStrategyRunning.load()) return;
    _isStrategyRunning.store(false);
    disp.strategyRunning.store(false);

    {
        SpinLockGuard hg(_hedgeLock);
        if (_totalHedgedForEntry > 0)
            _lastKnownOptQty = std::max(_lastKnownOptQty, _initialPosQty + _totalHedgedForEntry);
        _initialPosQty = _lastKnownOptQty;
    }

    int id = _activeEntryOrderId.exchange(0);
    if (id && _tradeClient && _tradeClient->isConnected())
        _tradeClient->cancelOrder(id, OrderCancel{});

    logf("AUTO-STOP: %s", reason);
}

void TradingEngine::stopStrategy(const char* reason) {
    SpinLockGuard guard(_processLock);
    stopStrategyLocked(reason);
}

void TradingEngine::panicCancelAll() {
    stopStrategy("PANIC");
    if (_tradeClient && _tradeClient->isConnected())
        _tradeClient->reqGlobalCancel(OrderCancel{});
    log("EMERGENCY STOP TRIGGERED");
}

void TradingEngine::closeAllPositionsMKT() {
    stopStrategy("Close All Positions (MKT)");
    log("CLOSE ALL MKT — implement position iteration here");
    // Iterate your local position map and placeOrder(..., "MKT") for each
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
        processPrice(_stockBid.load(std::memory_order_relaxed),
                     _stockAsk.load(std::memory_order_relaxed));
    }

    // 5. Update chain grid row (UI only — transparent lookup avoids std::string ctor)
    auto it = _iqChainMap.find(symbol);
    if (it != _iqChainMap.end()) {
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

    // Pre-filter 2 (Mode 2): skip spinlock when spot hasn't moved enough.
    // Safe: _lastProcessedBid is single-writer (this thread only) — no race.
    if (params.entryMode == 2 &&
        std::abs(stockBid - _lastProcessedBid) < params.spotSens) return;

    // Read clock before acquiring the lock — removes a syscall from the critical section.
    const auto now = Clock::now();
    DeferredLog dlog;
    {
        SpinLockGuard guard(_processLock);

        if (stockAsk <= 0 || stockBid <= 0 || std::isnan(stockAsk) ||
            std::isnan(stockBid) || stockAsk > params.maxSafePrice) return;

        // Advance the Mode-2 pre-filter baseline only after the tick is known
        // valid — a transient bad/over-cap ask must not move _lastProcessedBid,
        // or the spotSens filter would suppress later valid ticks at this bid.
        if (params.entryMode == 2) _lastProcessedBid = stockBid;

        const bool isCall = _isCall;
        const double mid  = (stockBid + stockAsk) * 0.5;
        const double intrinsic = isCall ? std::max(0.0, mid - _armedStrike)
                                        : std::max(0.0, _armedStrike - mid);

        double targetBid, currentEdge;
        if (params.entryMode == 1) {
            targetBid    = getLegalOptionPrice(params.staticPrice);
            currentEdge  = intrinsic - targetBid;
        } else {
            double raw   = intrinsic - params.dynDiscount;
            if (raw < 0.05) raw = 0.05;
            targetBid    = getLegalOptionPrice(raw);
            currentEdge  = intrinsic - targetBid;
        }

        disp.targetBid.store(targetBid,   std::memory_order_relaxed);
        disp.currentEdge.store(currentEdge, std::memory_order_relaxed);

        if (!_isStrategyRunning.load(std::memory_order_relaxed)) return;

        if (params.entryMode == 1) {
            if (currentEdge >= params.dynDiscount) {
                int activeId = _activeEntryOrderId.load(std::memory_order_relaxed);
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastEntryModTime).count();
                if (activeId == 0) {
                    if (ms > params.updateDelayMs) dlog = placeEntryOrder(targetBid, now);
                } else if (std::abs(_lastEntryPriceSent - targetBid) > 0.001) {
                    if (ms > params.updateDelayMs) dlog = updateEntryOrder(targetBid, now);
                }
            } else {
                if (_activeEntryOrderId.load(std::memory_order_relaxed) != 0)
                    stopStrategyLocked("Edge lost (Mode 1)");
            }
        } else {
            // Mode 2: _lastProcessedBid was committed above, after the validity gate
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastEntryModTime).count();
            if (ms > params.updateDelayMs) {
                int activeId = _activeEntryOrderId.load(std::memory_order_relaxed);
                if (activeId == 0)
                    dlog = placeEntryOrder(targetBid, now);
                else if (std::abs(_lastEntryPriceSent - targetBid) > 0.001)
                    dlog = updateEntryOrder(targetBid, now);
            }
        }
    } // _processLock released — safe to call onLog now
    if (dlog.pending) log(dlog.msg);
}

// ============================================================
// ORDER PLACEMENT — called with _processLock held
// Order and Contract objects are stack-allocated (no heap).
// ============================================================

TradingEngine::DeferredLog TradingEngine::placeEntryOrder(double price, TimePoint now) noexcept {
    DeferredLog dlog;
    if (_activeOptQty <= 0) return dlog;
    const double lp = price; // already getLegalOptionPrice() — computed once in processPrice/startStrategy

    // totalQuantity set once in startStrategy() — only price changes per order.
    _entryOrderTemplate.lmtPrice = lp;

    int newId = getNextOrderId();
    _activeEntryOrderId.store(newId);
    _lastEntryOrderId.store(newId);
    _tradeClient->placeOrder(newId, _optionContract, _entryOrderTemplate);
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
    const double lp = price; // already getLegalOptionPrice() — computed once in processPrice

    _entryOrderTemplate.lmtPrice = lp;
    _tradeClient->placeOrder(activeId, _optionContract, _entryOrderTemplate);
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

        if (_isHedgeComplete) return;

        double maxKnown = std::max({_sensorExecShares,
                                    _sensorOrderStatusFilled,
                                    _sensorPortfolioDiff});
        if (maxKnown > _activeOptQty) maxKnown = _activeOptQty;

        double unhedged = maxKnown - _totalHedgedForEntry;
        if (unhedged <= 0) return;

        _totalHedgedForEntry += unhedged;
        dlog = executeHedgeActual(unhedged);

        if (_totalHedgedForEntry >= _activeOptQty) {
            _isHedgeComplete = true;
            _isStrategyRunning.store(false);
            disp.strategyRunning.store(false);
            targetFilled = true;
        }
    } // _hedgeLock released — safe to call onLog now
    if (targetFilled)  log("TARGET FILLED — auto-stop");
    if (dlog.pending)  log(dlog.msg);
}

TradingEngine::DeferredLog TradingEngine::executeHedgeActual(double optShares) noexcept {
    DeferredLog dlog;
    if (!_contractsSet.load(std::memory_order_relaxed)) return dlog;

    double sBid = _stockBid.load(std::memory_order_relaxed);
    double sAsk = _stockAsk.load(std::memory_order_relaxed);

    double limitPrice;
    Order& htmpl = _isCall ? _hedgeCallTemplate : _hedgePutTemplate;
    if (_isCall)
        limitPrice = (sAsk > 0) ? sAsk - params.hedgeOffset : sBid;
    else
        limitPrice = (sAsk > 0) ? sAsk + params.hedgeOffset : sBid;
    double safePx = std::round(limitPrice * 100.0) / 100.0;

    double qty = optShares * params.hedgeQty;

    htmpl.totalQuantity = qty;
    htmpl.lmtPrice      = safePx;

    int hid = getNextOrderId();
    _activeHedgeOrderId.store(hid);
    _lastHedgeOrderId.store(hid);
    _tradeClient->placeOrder(hid, _hedgeContract, htmpl);

    snprintf(dlog.msg, sizeof(dlog.msg), "HEDGE: %s %.0f STK @ %.2f (id=%d)",
             htmpl.action.c_str(), qty, safePx, hid);
    dlog.pending = true;
    return dlog;
}

// ============================================================
// IBKR EWrapper CALLBACKS (meaningful implementations)
// ============================================================

void TradingEngine::nextValidId(int orderId) {
    if (orderId > _nextOrderId.load())
        _nextOrderId.store(orderId);
}

void TradingEngine::currentTime(long long /*time*/) {
    auto now = Clock::now();
    if (_ibPingReqTime != TimePoint{}) {
        int ms = (int)std::chrono::duration_cast<std::chrono::milliseconds>(now - _ibPingReqTime).count();
        disp.ibPingMs.store(ms);
    }
    _ibPingReqTime = now;
    _tradeClient->reqCurrentTime();
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

    if (id > 0) {
        bool isFatal = (errorCode == 201 || errorCode == 202 ||
                        errorCode == 104 || errorCode == 203 || errorCode == 399);
        if (isFatal) {
            if (id == _activeEntryOrderId.load()) {
                _activeEntryOrderId.store(0);
                stopStrategy("IBKR rejected order");
            } else if (id == _activeHedgeOrderId.load()) {
                _activeHedgeOrderId.store(0);
            }
        }
    }
}

void TradingEngine::execDetails(int /*reqId*/, const Contract& contract,
                                 const Execution& exec) {
    // Dedup: the same execution can arrive more than once
    if (_processedExecIds.count(exec.execId)) return;

    bool isOurEntry = (exec.orderId == (long long)_lastEntryOrderId.load() &&
                       _lastEntryOrderId.load() != 0);
    if (!isOurEntry && _isStrategyRunning.load() && _contractsSet.load(std::memory_order_relaxed)) {
        // Match by contract attributes as fallback
        if (contract.symbol == _optionContract.symbol &&
            std::abs(contract.strike - _optionContract.strike) < 0.001 &&
            contract.right == _optionContract.right &&
            exec.side == "BOT")
            isOurEntry = true;
    }
    bool isOurHedge = (!isOurEntry &&
                       exec.orderId == (long long)_lastHedgeOrderId.load() &&
                       _lastHedgeOrderId.load() != 0);

    if (isOurEntry) {
        SpinLockGuard hg(_hedgeLock);
        _processedExecIds.insert(exec.execId);
        _sensorExecShares += exec.shares;
        if (_lastOptionFillTime == TimePoint{}) _lastOptionFillTime = Clock::now();
        // Release hedge lock before evaluateHedge (it re-acquires)
    }
    if (isOurEntry) evaluateHedge();

    if (isOurHedge) {
        SpinLockGuard hg(_hedgeLock);
        _processedExecIds.insert(exec.execId);
        if (_lastOptionFillTime != TimePoint{}) {
            double ms = std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - _lastOptionFillTime).count() / 1000.0;
            logf("[LATENCY OPT->STK HEDGE] %.2f ms", ms);
            _lastOptionFillTime = {};
        }
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
            if (status == "Filled")
                stopStrategy("Entry completely filled");
            else
                stopStrategy(("Entry terminal: " + status).c_str());
        }
    }

    if (orderId == _activeHedgeOrderId.load() && isTerminal) {
        _activeHedgeOrderId.store(0);
        logf("Hedge %d: %s", orderId, status.c_str());
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
    if (!_contractsSet.load(std::memory_order_relaxed)) return;
    if (contract.symbol == _optionContract.symbol &&
        std::abs(contract.strike - _optionContract.strike) < 0.001 &&
        contract.right == _optionContract.right) {
        SpinLockGuard hg(_hedgeLock);
        _lastKnownOptQty = DecimalFunctions::decimalToDouble(pos);
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

double TradingEngine::getLegalOptionPrice(double price) noexcept {
    if (price < 3.00)
        return std::round(price * 100.0) / 100.0;
    // Options above $3.00 tick in $0.05 increments
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
