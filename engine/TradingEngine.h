#pragma once
// ============================================================
// TradingEngine.h — Ultra-low latency HFT engine
//
// Dependencies:
//   - IBKR C++ API (TWS API 10.19+) — place headers in IBApi/
//   - ThetaDataClient (WebSocket, ThetaTerminal on localhost:25520)
//   - SpinLock (cache-line-aligned spinlock)
//
// Requires: C++17, MSVC or Clang/LLVM on Windows
// ============================================================

#include <atomic>
#include <thread>
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <chrono>
#include <cstring>
#include <cmath>
#include <memory>
#include <cstdio>

// IBKR C++ TWS API headers (TWS API 9.79+, with protobuf support)
#include "../IBApi/DefaultEWrapper.h"
#include "../IBApi/EClientSocket.h"
#include "../IBApi/EReaderOSSignal.h"
#include "../IBApi/EReader.h"
#include "../IBApi/Contract.h"
#include "../IBApi/Order.h"
#include "../IBApi/OrderCancel.h"
#include "../IBApi/Execution.h"
#include "../IBApi/OrderState.h"
#include "../IBApi/CommonDefs.h"
#include "../IBApi/Decimal.h"

#include <mutex>
#include "SpinLock.h"
#include "ThetaDataClient.h"

using Clock     = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

// -----------------------------------------------------------------------
// Per-row data for the options chain grid.
// All live price fields are atomics so the ThetaData recv thread can write
// while the UI thread reads, with no locking.
// -----------------------------------------------------------------------
struct alignas(64) ChainRowData {
    int    rowIndex  = 0;
    double strike    = 0.0;
    char   expiration[16] = {};
    char   callOsi[40]    = {};
    char   putOsi[40]     = {};

    // Call side — on its own cache line
    alignas(64)
    std::atomic<double> callBid{0};
    std::atomic<double> callAsk{0};
    std::atomic<double> callLast{0};
    std::atomic<int>    callVol{0};
    std::atomic<bool>   callReceived{false};

    // Put side — on a separate cache line
    alignas(64)
    std::atomic<double> putBid{0};
    std::atomic<double> putAsk{0};
    std::atomic<double> putLast{0};
    std::atomic<int>    putVol{0};
    std::atomic<bool>   putReceived{false};

    // std::atomic is not movable; provide explicit move ctor so vector can reallocate.
    ChainRowData() = default;
    ChainRowData(ChainRowData&& o) noexcept
        : rowIndex(o.rowIndex), strike(o.strike),
          callBid(o.callBid.load(std::memory_order_relaxed)),
          callAsk(o.callAsk.load(std::memory_order_relaxed)),
          callLast(o.callLast.load(std::memory_order_relaxed)),
          callVol(o.callVol.load(std::memory_order_relaxed)),
          callReceived(o.callReceived.load(std::memory_order_relaxed)),
          putBid(o.putBid.load(std::memory_order_relaxed)),
          putAsk(o.putAsk.load(std::memory_order_relaxed)),
          putLast(o.putLast.load(std::memory_order_relaxed)),
          putVol(o.putVol.load(std::memory_order_relaxed)),
          putReceived(o.putReceived.load(std::memory_order_relaxed))
    {
        std::memcpy(expiration, o.expiration, sizeof(expiration));
        std::memcpy(callOsi,    o.callOsi,    sizeof(callOsi));
        std::memcpy(putOsi,     o.putOsi,     sizeof(putOsi));
    }
    ChainRowData(const ChainRowData&) = delete;
    ChainRowData& operator=(const ChainRowData&) = delete;
    ChainRowData& operator=(ChainRowData&&) = delete;
};

struct IqOptionMapInfo {
    ChainRowData* rowData = nullptr;
    bool          isCall  = false;
};

// Custom hash/equal that support heterogeneous (const char*) lookup on an
// unordered_map<string, ...> without constructing a temporary std::string.
// This removes heap allocation from the hot-path chain map lookup.
struct TransparentStringHash {
    using is_transparent = void;
    size_t operator()(const std::string&    s) const noexcept { return std::hash<std::string>{}(s); }
    size_t operator()(std::string_view      s) const noexcept { return std::hash<std::string_view>{}(s); }
    size_t operator()(const char*           s) const noexcept { return std::hash<std::string_view>{}(s); }
};
struct TransparentStringEqual {
    using is_transparent = void;
    bool operator()(const std::string& a, const std::string& b) const noexcept { return a == b; }
    bool operator()(const char*        a, const std::string& b) const noexcept { return b == a; }
    bool operator()(const std::string& a, const char*        b) const noexcept { return a == b; }
    bool operator()(std::string_view   a, const std::string& b) const noexcept { return b == a; }
};
using ChainMap = std::unordered_map<std::string, IqOptionMapInfo,
                                    TransparentStringHash, TransparentStringEqual>;

// -----------------------------------------------------------------------
// Immutable-structure chain snapshot (RCU pattern).
// loadChain() builds a complete new snapshot and publishes it with an atomic
// shared_ptr store; the ThetaData recv thread loads the pointer per quote and
// writes only the atomic price fields inside rows. The old snapshot is freed
// when its last reader drops the reference — no reader ever sees a vector
// being cleared/reallocated under it.
// -----------------------------------------------------------------------
struct ChainSnapshot {
    std::vector<ChainRowData> rows;
    ChainMap                  map;   // symbol → pointer into rows (stable for snapshot lifetime)
};

// -----------------------------------------------------------------------
// Append-only on-disk audit journal: every order placement/update, fill,
// rejection and strategy start/stop. Independent of the UI log socket so a
// session can always be reconstructed. Never called while a spinlock is held.
// -----------------------------------------------------------------------
class AuditLog {
public:
    ~AuditLog();
    void write(const char* fmt, ...);
private:
    FILE*      _f = nullptr;
    std::mutex _m;
};

// -----------------------------------------------------------------------
// TradingEngine
// Implements EWrapper so IBKR callbacks arrive directly into this class.
// -----------------------------------------------------------------------
class TradingEngine : public DefaultEWrapper {
public:
    TradingEngine();
    ~TradingEngine() override;

    // ------------------------------------------------------------------
    // Connection
    // ------------------------------------------------------------------
    bool connectIB(const char* host = "127.0.0.1", int tradePort = 4002);
    void disconnectIB();
    bool connectIQ(const char* host = "127.0.0.1", uint16_t port = 25520);
    void disconnectIQ();

    bool ibConnected()  const noexcept;
    bool iqConnected()  const noexcept { return _thetaClient.isConnected(); }

    // ------------------------------------------------------------------
    // Chain management
    // ------------------------------------------------------------------
    void searchContract(const char* symbol);
    void loadChain(const std::string& expiration, double spotFilterPct = 0.15);

    // ------------------------------------------------------------------
    // Arming: call after double-clicking a chain row
    // ------------------------------------------------------------------
    void armContract(const char* symbol, const char* expiry,
                     double strike, bool isCall,
                     const char* hedgeSymbol = nullptr);

    // ------------------------------------------------------------------
    // Strategy
    // ------------------------------------------------------------------
    bool startStrategy();
    void stopStrategy(const char* reason);   // thread-safe
    void panicCancelAll();
    void closeAllPositionsMKT();

    // ------------------------------------------------------------------
    // Tunable parameters. All atomic: written by the ControlServer thread at
    // runtime while the hot path reads them — plain doubles would be a data
    // race. Atomic loads compile to plain MOVs on x86, so the hot path cost
    // is unchanged. entryMode/optQty changes are rejected while the strategy
    // is running (see ControlServer::applyParam).
    // ------------------------------------------------------------------
    struct Params {
        std::atomic<int>    entryMode{2};      // 1 = static price, 2 = dynamic discount
        std::atomic<double> staticPrice{5.0};
        std::atomic<double> dynDiscount{0.10};
        std::atomic<int>    updateDelayMs{2000};
        std::atomic<double> spotSens{0.05};
        std::atomic<double> hedgeOffset{0.05};
        std::atomic<double> maxSafePrice{1000.0};
        std::atomic<double> optQty{1.0};
        std::atomic<double> hedgeQty{100.0};

        // Tick rounding: 0 = nickel above $3 (default), 1 = penny at all prices
        // (penny-interval-program classes like SPY quote in pennies everywhere).
        std::atomic<int>    tickMode{0};

        // Safety / risk limits
        std::atomic<int>    staleMs{3000};            // stop strategy if no relevant tick for this long (0 = off)
        std::atomic<int>    hedgeChaseMs{1500};       // reprice an unfilled hedge after this long
        std::atomic<int>    maxHedgeChases{3};        // limit-price chases before escalating to MKT
        std::atomic<int>    maxHedgeRetries{5};       // re-placements after reject/cancel before alarm
        std::atomic<double> maxOptQty{10.0};          // refuse to start with optQty above this
        std::atomic<int>    maxOrdersPerRun{500};     // entry placements per strategy run (runaway guard)
        std::atomic<int>    maxConsecRejects{3};      // consecutive fatal rejects -> global cancel
    } params;

    // ------------------------------------------------------------------
    // Observable state (lock-free reads from UI thread)
    // ------------------------------------------------------------------
    struct alignas(64) DisplayState {
        std::atomic<double> stockBid{0};
        std::atomic<double> stockAsk{0};
        std::atomic<double> optBid{0};
        std::atomic<double> optAsk{0};
        std::atomic<double> oppOptBid{0};
        std::atomic<double> oppOptAsk{0};
        std::atomic<double> targetBid{0};
        std::atomic<double> currentEdge{0};
        std::atomic<bool>   strategyRunning{false};
        std::atomic<int>    ibPingMs{0};
    } disp;

    // Current chain snapshot (may be null before the first loadChain).
    // Readers hold the returned shared_ptr for the duration of their access.
    std::shared_ptr<const ChainSnapshot> chainSnapshot() const {
        return std::atomic_load_explicit(&_chainSnap, std::memory_order_acquire);
    }

    // Expiration list populated after searchContract
    std::vector<std::string> expirations;

    // Incremented when chain/expirations data changes — lets ControlServer detect updates.
    std::atomic<int> chainVersion{0};
    std::atomic<int> expVersion{0};

    // Protects the expirations vector (chain rows are published via chainSnapshot()).
    // Held briefly; never on hot path.
    std::mutex chainMutex;

    // Managed accounts
    char mainAccount[32] = {};

    // Log callback — fired from various threads; keep it non-blocking
    std::function<void(const char* msg)> onLog;

private:
    // ------------------------------------------------------------------
    // IBKR sockets
    // ------------------------------------------------------------------
    EReaderOSSignal _tradeSignal, _infoSignal;
    EClientSocket*       _tradeClient  = nullptr;
    EClientSocket*       _infoClient   = nullptr;
    EReader*             _tradeReader  = nullptr;
    EReader*             _infoReader   = nullptr;
    std::thread          _tradeThread, _infoThread;
    int                  _clientId     = 0;

    // ------------------------------------------------------------------
    // ThetaData
    // ------------------------------------------------------------------
    ThetaDataClient _thetaClient;
    std::atomic<bool> _iqShouldBeConnected{false}; // user intent — drives watchdog auto-reconnect
    char       _iqHost[64] = "127.0.0.1";
    uint16_t   _iqPort     = 25520;
    std::mutex _iqSymMutex;   // protects _activeIqSymbols (control threads + watchdog)

    // ------------------------------------------------------------------
    // HOT PRICE STATE — each pair on its own cache line to prevent false sharing
    // Written by ThetaData recv thread, read by processPrice spinlock holder.
    // ------------------------------------------------------------------
    alignas(64) std::atomic<double> _stockBid{0};
                std::atomic<double> _stockAsk{0};
    alignas(64) std::atomic<double> _optBid{0};
                std::atomic<double> _optAsk{0};
    alignas(64) std::atomic<double> _oppOptBid{0};
                std::atomic<double> _oppOptAsk{0};

    // ------------------------------------------------------------------
    // ORDER ID — atomic counter, no lock needed
    // ------------------------------------------------------------------
    alignas(64) std::atomic<int> _nextOrderId{1};  // start at 1: id 0 is the "no active order" sentinel
                std::atomic<int> _activeEntryOrderId{0};
                std::atomic<int> _lastEntryOrderId{0};

    // ------------------------------------------------------------------
    // STRATEGY STATE — under _processLock or _hedgeLock respectively
    // ------------------------------------------------------------------
    alignas(64) std::atomic<bool> _isStrategyRunning{false};

    // _activeOptQty: written at startStrategy (_processLock), read by both the
    // entry path (_processLock) and the hedge path (_hedgeLock) — atomic so the
    // cross-lock read is well-defined.
    std::atomic<double> _activeOptQty{0.0};

    // Protected by _processLock (atomic: the Mode-2 pre-filter reads it before
    // taking the lock, and startStrategy resets it from the control thread)
    std::atomic<double> _lastProcessedBid{0.0};
    double    _lastEntryPriceSent    = 0.0;
    TimePoint _lastEntryModTime      = {};

    // Protected by _hedgeLock
    double    _sensorExecShares      = 0.0;
    double    _sensorOrderStatusFilled = 0.0;
    double    _sensorPortfolioDiff   = 0.0;
    double    _hedgePlacedOptQty     = 0.0;  // option-contract equivalents covered by PLACED hedge orders
    double    _hedgeFilledShares     = 0.0;  // stock shares actually FILLED across hedge orders
    int       _hedgeRetries          = 0;    // re-placements after reject/cancel (this run)
    bool      _hedgeAlarmed          = false;// retry budget exhausted — manual action required
    bool      _entryFillComplete     = false;// full optQty confirmed filled
    bool      _isHedgeComplete       = false;// all hedge SHARES confirmed filled
    double    _initialPosQty         = 0.0;
    double    _lastKnownOptQty       = 0.0;
    TimePoint _lastOptionFillTime    = {};

    // Per-hedge-order accounting — placed vs filled, for shortfall retry and
    // the watchdog chase. Under _hedgeLock.
    struct HedgeOrderRec {
        double    qtyShares    = 0.0;
        double    filledShares = 0.0;
        double    lmtPrice     = 0.0;
        int       chases       = 0;
        bool      open         = false;
        TimePoint placedAt     = {};
    };
    std::unordered_map<int, HedgeOrderRec> _hedgeOrders;

    // ------------------------------------------------------------------
    // RISK COUNTERS
    // ------------------------------------------------------------------
    std::atomic<int> _entryOrdersThisRun{0}; // runaway guard vs maxOrdersPerRun
    std::atomic<int> _consecRejects{0};      // kill switch vs maxConsecRejects

    // ------------------------------------------------------------------
    // SPINLOCKS — each on its own cache line (see SpinLock.h)
    // ------------------------------------------------------------------
    SpinLock _processLock;  // guards price processing & order placement
    SpinLock _hedgeLock;    // guards hedge accounting
    // Serializes all sends on the IBKR sockets. EClientSocket is not
    // thread-safe and orders are placed from three threads (ThetaData recv,
    // IBKR reader callbacks, watchdog chase). Uncontended cost ~10 ns.
    SpinLock _ibSendLock;

    // ------------------------------------------------------------------
    // CONTRACTS — set once by armContract(), then read-only during trading
    // ------------------------------------------------------------------
    Contract _optionContract   = {};
    Contract _oppositeContract = {};
    Contract _hedgeContract    = {};
    std::atomic<bool> _contractsSet{false};
    bool     _isCall           = false;  // cached at armContract(); avoids std::string cmp on hot path
    double   _armedStrike      = 0.0;   // cached at armContract(); avoids cache miss on large Contract struct

    // Pre-built Order templates — populated once at armContract() / startStrategy().
    // placeEntryOrder() only writes lmtPrice then calls placeOrder(); all other
    // fields are already set, eliminating 8+ std::string assignments on the hot path.
    // Two hedge templates pre-set action="SELL"/"BUY" at arm time — eliminates
    // the std::string write from inside _hedgeLock on every hedge placement.
    alignas(64) Order _entryOrderTemplate  = {};
    alignas(64) Order _hedgeCallTemplate   = {};  // action="SELL" — short hedge for calls
    alignas(64) Order _hedgePutTemplate    = {};  // action="BUY"  — long hedge for puts

    // Pre-allocated fixed-size symbol key buffers — avoid std::string in hot path.
    // Format: "{root}{YYYYMMDD}{C|P}{strike_millicents}" — see ThetaDataClient.h
    char _armedOptSymbol[40] = {};   // ThetaData key for the armed option
    char _armedOppSymbol[40] = {};   // ThetaData key for the opposite leg
    char _stockSymbol[32]    = {};   // underlying stock ticker

    // ------------------------------------------------------------------
    // CHAIN DATA  (loaded once; protected by _chainDataLock)
    // ------------------------------------------------------------------
    struct ChainEntry { std::set<double> strikes; };
    std::unordered_map<std::string, ChainEntry> _chainData;
    SpinLock _chainDataLock;

    // Published chain snapshot — see ChainSnapshot. Accessed only through
    // std::atomic_load / std::atomic_store.
    std::shared_ptr<ChainSnapshot> _chainSnap;

    std::unordered_set<std::string> _activeIqSymbols;  // under _iqSymMutex

    int _searchReqId      = 9000;
    int _lastChainReqId   = 0;
    int _currentUndConId  = 0;

    std::unordered_set<std::string> _processedExecIds; // dedup exec callbacks

    // ------------------------------------------------------------------
    // ACCOUNT POSITIONS — fed by position() callback; used by closeAllPositionsMKT
    // ------------------------------------------------------------------
    struct PosRec { Contract con; double qty = 0.0; };
    std::unordered_map<std::string, PosRec> _positions;  // under _posMutex
    std::mutex _posMutex;

    // ------------------------------------------------------------------
    // WATCHDOG — below-normal-priority thread: data staleness stop,
    // ThetaData auto-reconnect, hedge chase, IB ping initiation.
    // ------------------------------------------------------------------
    std::thread            _watchThread;
    std::atomic<bool>      _watchRun{false};
    std::atomic<uint64_t>  _tickSeq{0};        // ++ per trading-relevant quote (relaxed)
    std::atomic<long long> _ibPingReqNs{0};    // steady_clock ns of last reqCurrentTime

    // Stop events happen while _processLock is held; the audit write (fprintf)
    // must not run inside a spinlock, so the reason is parked here and flushed
    // by the watchdog thread.
    char              _stopReason[96] = {};
    std::atomic<bool> _stopAuditPending{false};

    // ------------------------------------------------------------------
    // AUDIT JOURNAL
    // ------------------------------------------------------------------
    AuditLog _audit;

    // ------------------------------------------------------------------
    // CORE HOT-PATH METHODS
    // ------------------------------------------------------------------

    // Filled inside a spinlock section, fired by the caller after the lock releases.
    // Keeps onLog (which may do socket I/O) off the critical path.
    struct DeferredLog {
        char msg[128] = {};
        bool pending  = false;
    };

    void processIQData(const char* symbol, double bid, double ask, double last, int vol) noexcept;
    void processPrice(double stockBid, double stockAsk) noexcept;
    DeferredLog placeEntryOrder(double price, TimePoint now) noexcept;
    DeferredLog updateEntryOrder(double price, TimePoint now) noexcept;
    void evaluateHedge() noexcept;
    DeferredLog executeHedgeActual(double optShares) noexcept;

    // Watchdog internals
    void watchdogLoop() noexcept;
    void chaseHedges(TimePoint now) noexcept;

    // Fire a DeferredLog after the lock that produced it was released:
    // UI log + audit journal.
    void emitDeferred(const DeferredLog& d);

    // ------------------------------------------------------------------
    // HELPERS
    // ------------------------------------------------------------------
    double getLegalOptionPrice(double price) const noexcept;
    double getCurrentSpot() const noexcept;
    std::string buildThetaOptionKey(const char* sym, const char* ibkrExpiry, char right, double strike) const;
    int  getNextOrderId() noexcept;
    void log(const char* msg) const;
    void logf(const char* fmt, ...) const;

    // Internal stop without re-acquiring _processLock (called while lock is held)
    void stopStrategyLocked(const char* reason) noexcept;

    // ------------------------------------------------------------------
    // EWrapper overrides — only methods with real logic; all others are
    // handled by DefaultEWrapper's empty implementations.
    // ------------------------------------------------------------------
public:
    void nextValidId(int orderId) override;

    void error(int id, time_t errorTime, int errorCode,
               const std::string& errorStr, const std::string& json) override;

    void execDetails(int reqId, const Contract& contract,
                     const Execution& exec) override;

    void orderStatus(int orderId, const std::string& status,
                     Decimal filled, Decimal remaining, double avgFillPrice,
                     long long permId, int parentId, double lastFillPrice,
                     int clientId, const std::string& whyHeld,
                     double mktCapPrice) override;

    void contractDetails(int reqId, const ContractDetails& details) override;

    void securityDefinitionOptionalParameter(
        int reqId, const std::string& exchange, int underlyingConId,
        const std::string& tradingClass, const std::string& multiplier,
        const std::set<std::string>& expirations,
        const std::set<double>& strikes) override;

    void securityDefinitionOptionalParameterEnd(int reqId) override;

    void managedAccounts(const std::string& accountsList) override;

    void position(const std::string& account, const Contract& contract,
                  Decimal pos, double avgCost) override;

    void updatePortfolio(const Contract& contract, Decimal position,
                         double marketPrice, double marketValue,
                         double averageCost, double unrealizedPNL,
                         double realizedPNL,
                         const std::string& accountName) override;

    void currentTime(long long time) override;
};
