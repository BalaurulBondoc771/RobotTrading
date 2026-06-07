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
    // Tunable parameters (set before startStrategy)
    // ------------------------------------------------------------------
    struct Params {
        int    entryMode     = 2;      // 1 = static price, 2 = dynamic discount
        double staticPrice   = 5.0;
        double dynDiscount   = 0.10;
        int    updateDelayMs = 2000;
        double spotSens      = 0.05;
        double hedgeOffset   = 0.05;
        double maxSafePrice  = 1000.0;
        double optQty        = 1.0;
        double hedgeQty      = 100.0;
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

    // Chain rows — written once at load time, then live fields updated via atomics
    std::vector<ChainRowData> chainRows;

    // Expiration list populated after searchContract
    std::vector<std::string> expirations;

    // Incremented when chain/expirations data changes — lets ControlServer detect updates.
    std::atomic<int> chainVersion{0};
    std::atomic<int> expVersion{0};

    // Protects chainRows and expirations structural access (not atomic fields within rows).
    // Held briefly by loadChain() and ControlServer status broadcast; never on hot path.
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
                std::atomic<int> _activeHedgeOrderId{0};
                std::atomic<int> _lastHedgeOrderId{0};

    // ------------------------------------------------------------------
    // STRATEGY STATE — under _processLock or _hedgeLock respectively
    // ------------------------------------------------------------------
    alignas(64) std::atomic<bool> _isStrategyRunning{false};

    // Protected by _processLock
    double    _activeOptQty          = 0.0;
    double    _lastEntryPriceSent    = 0.0;
    double    _lastProcessedBid      = 0.0;
    TimePoint _lastEntryModTime      = {};

    // Protected by _hedgeLock
    double    _sensorExecShares      = 0.0;
    double    _sensorOrderStatusFilled = 0.0;
    double    _sensorPortfolioDiff   = 0.0;
    double    _totalHedgedForEntry   = 0.0;
    bool      _isHedgeComplete       = false;
    double    _initialPosQty         = 0.0;
    double    _lastKnownOptQty       = 0.0;
    TimePoint _lastOptionFillTime    = {};

    // ------------------------------------------------------------------
    // SPINLOCKS — each on its own cache line (see SpinLock.h)
    // ------------------------------------------------------------------
    SpinLock _processLock;  // guards price processing & order placement
    SpinLock _hedgeLock;    // guards hedge accounting

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

    ChainMap                _iqChainMap;       // symbol → row, transparent lookup
    std::unordered_set<std::string> _activeIqSymbols;

    int _searchReqId      = 9000;
    int _lastChainReqId   = 0;
    int _currentUndConId  = 0;

    std::unordered_set<std::string> _processedExecIds; // dedup exec callbacks

    // Ping timing
    TimePoint _ibPingReqTime = {};

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

    // ------------------------------------------------------------------
    // HELPERS
    // ------------------------------------------------------------------
    static double getLegalOptionPrice(double price) noexcept;
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
