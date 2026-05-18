# RobotTrading — HFT Options Engine

A high-frequency options trading engine written in C++17, targeting sub-10 µs latency from market data arrival to order placement. Runs on **Windows** (MSVC) and **Linux** (GCC/Clang) from a single codebase.

Connects to **Interactive Brokers TWS API** for order execution and **ThetaData Terminal** for real-time options market data.

---

## Architecture

```
                  ┌─────────────────────────────────────────┐
                  │           robottrading_engine            │
                  │                                          │
  ThetaData  ───► │  ThetaDataClient   ──►  TradingEngine   │ ───► IBKR TWS
  Terminal        │  (WebSocket, core 3)    (SpinLock core) │      (trade socket, core 2)
  :25520          │                                          │      :4002
                  │  ControlServer :7799                     │
                  └──────────────┬──────────────────────────┘
                                 │ TCP (localhost)
                  ┌──────────────▼──────────────────────────┐
                  │           robottrading_ui                │
                  │  HTTP server :8080                       │
                  │  EngineProxy (connects to :7799)         │
                  └─────────────────────────────────────────┘
                                 ▲
                           Browser (any)
```

**Two separate processes** communicate over localhost TCP:
- `robottrading_engine` — all trading logic, zero UI dependencies
- `robottrading_ui` — HTTP server serving the web interface, proxies commands to the engine

---

## Prerequisites

### Both platforms
| Dependency | Version | Notes |
|---|---|---|
| CMake | 3.16+ | Build system |
| IBKR TWS C++ API | 10.19+ | See *IBKR API setup* below |
| ThetaData Terminal | latest | Must be running on `localhost:25520` |

### Windows
| Dependency | Notes |
|---|---|
| Visual Studio 2022 | C++ Desktop workload required |
| vcpkg | For protobuf (`C:\vcpkg`) |

### Linux
| Dependency | Install |
|---|---|
| GCC 11+ or Clang 14+ | `sudo apt install build-essential` |
| CMake | `sudo apt install cmake` |
| protobuf dev | `sudo apt install libprotobuf-dev protobuf-compiler` |
| pthread | Included in glibc |

---

## IBKR API setup (required on both platforms)

The IBKR C++ API is **not redistributable** and must be downloaded manually.

1. Go to https://interactivebrokers.github.io/ → *TWS API* → download the installer
2. Install it (or extract the zip)
3. Copy the contents of `cppclient/client/` into the `IBApi/` folder in this repo:

```
RobotTrading/
└── IBApi/
    ├── EClientSocket.cpp
    ├── EClientSocket.h
    ├── EWrapper.h
    ├── Contract.h
    ├── Order.h
    ├── ... (all .cpp and .h files from cppclient/client/)
    └── protobuf/
        ├── *.cc
        └── *.h
```

---

## Build — Windows (MSVC + vcpkg)

### 1. Install vcpkg and protobuf
```cmd
git clone https://github.com/microsoft/vcpkg C:\vcpkg
C:\vcpkg\bootstrap-vcpkg.bat
C:\vcpkg\vcpkg.exe install protobuf:x64-windows
```

### 2. Configure
```cmd
cd C:\develop\RobotTrading
cmake -B build -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### 3. Build both targets
```cmd
cmake --build build --config Release
```

Outputs:
- `build\Release\robottrading_engine.exe`
- `build\Release\robottrading_ui.exe`

---

## Build — Linux (GCC/Clang)

### 1. Install dependencies
```bash
sudo apt update
sudo apt install build-essential cmake libprotobuf-dev protobuf-compiler
```

### 2. Configure
```bash
cd ~/RobotTrading
cmake -B build -DCMAKE_BUILD_TYPE=Release
```

### 3. Build
```bash
cmake --build build --jobs $(nproc)
```

Outputs:
- `build/robottrading_engine`
- `build/robottrading_ui`

### 4. (Optional but recommended) Real-time scheduling privileges

Without this, the OS may preempt trading threads, adding 100 µs–1 ms jitter.

**Option A — persistent (survives reboot):** Add to `/etc/security/limits.conf`:
```
<your_username>  -  rtprio   99
<your_username>  -  memlock  unlimited
```
Log out and back in, then run the engine normally.

**Option B — one-time capability grant:**
```bash
sudo setcap cap_sys_nice+ep ./build/robottrading_engine
```

**Option C — run with sudo (not recommended for production):**
```bash
sudo ./build/robottrading_engine
```

---

## Running — step by step

### Step 1 — Start Interactive Brokers TWS or IB Gateway
- Enable API access: *Edit → Global Configuration → API → Settings*
- Check "Enable ActiveX and Socket Clients"
- Set Socket port to **4002** (paper trading) or **7496** (live)
- Uncheck "Read-Only API" if you want order placement

### Step 2 — Start ThetaData Terminal
- Download from https://thetadata.net
- Launch it — it listens on `localhost:25520` by default
- Log in with your ThetaData credentials

### Step 3 — Start the engine
```bash
# Linux
./build/robottrading_engine

# Windows
build\Release\robottrading_engine.exe
```

Expected output:
```
[engine] ControlServer listening on :7799
[engine] Start robottrading_ui and open http://localhost:8080
```

### Step 4 — Start the UI
```bash
# Linux
./build/robottrading_ui

# Windows
build\Release\robottrading_ui.exe
```

### Step 5 — Open the web interface
Navigate to `http://localhost:8080` in any browser.

### Step 6 — Connect and trade
1. Click **Connect IB** → connects to TWS/Gateway on port 4002
2. Click **Connect ThetaData** → connects to ThetaData Terminal
3. Enter a ticker (e.g. `SPY`) and click **Search** → loads the options chain
4. Select an expiration → strike filter auto-applies
5. Click a row in the chain to **arm** a contract
6. Configure parameters (see below)
7. Click **Start** to begin the strategy

---

## Trading parameters

| Parameter | Default | Description |
|---|---|---|
| `entryMode` | `2` | `1` = static price, `2` = dynamic discount from intrinsic value |
| `staticPrice` | `5.00` | Target entry price for Mode 1 |
| `dynDiscount` | `0.10` | Discount below intrinsic value for Mode 2 entry |
| `updateDelayMs` | `2000` | Minimum ms between order updates |
| `spotSens` | `0.05` | Mode 2: minimum stock price move (in $) to re-evaluate |
| `hedgeOffset` | `0.05` | Slippage offset for hedge limit price |
| `optQty` | `1.0` | Option contracts to buy per entry |
| `hedgeQty` | `100.0` | Stock shares per hedge order |

Parameters are set at startup in `main.cpp` and can be changed at runtime via the UI.

### Mode 1 (Static price)
Places a limit buy at `staticPrice`. Checks that `intrinsic_value - staticPrice >= dynDiscount` as a safety gate before placing.

### Mode 2 (Dynamic discount)
Continuously tracks `target = intrinsic_value - dynDiscount`. Only processes a tick if the stock bid has moved by at least `spotSens`, eliminating ~85–90% of ticks before entering the spinlock.

---

## Project structure

```
RobotTrading/
├── main.cpp                    Engine process entry point
├── webui_main.cpp              UI process entry point
├── CMakeLists.txt              Build system (two targets)
├── vcpkg.json                  vcpkg manifest (protobuf dependency)
│
├── engine/
│   ├── platform.h              Cross-platform OS primitives (Windows / Linux)
│   ├── SpinLock.h              Cache-line-aligned spinlock with HFT_PAUSE()
│   ├── TradingEngine.h/.cpp    Core trading logic, IBKR EWrapper callbacks
│   ├── ThetaDataClient.h/.cpp  WebSocket client for market data
│   ├── ControlServer.h/.cpp    TCP command server (:7799), talks to UI
│   ├── bid64_impl.cpp          Fixed-point price utilities
│   └── IQFeedClient.h/.cpp     IQFeed client (reference, not used in active path)
│
├── webui/
│   ├── HttpServer.h/.cpp       HTTP/WebSocket server (:8080)
│   └── EngineProxy.h/.cpp      Proxies commands to engine :7799
│
└── IBApi/                      IBKR TWS C++ API — place here manually (see above)
```

---

## Performance optimizations

The engine is tuned to minimize the latency from a ThetaData quote arriving to the IBKR order leaving the NIC. The dominant bottleneck is the TCP write to IBKR (~4–18 µs). All user-space optimizations target the path before that write.

### 1. Single-pass JSON parser
**Where:** `ThetaDataClient::processMessage()`

The original approach called a `findValue()` scan once per field (9 separate linear scans per message). The single-pass parser walks the JSON exactly once and extracts all fields simultaneously using a `switch` on key length + `memcmp`. Eliminated 8 redundant scans per quote message.

**Saving: ~800–1200 ns per message**

---

### 2. Fast numeric parsers
**Where:** `ThetaDataClient.cpp` — `fast_atof()`, `fast_atoll()`

MSVC's `atof()` handles locale, exponents, infinity, NaN — costs ~100–300 ns per call. Our options prices are plain decimals (`123.45`) with no exponents. The hand-rolled parsers handle only `"-?[0-9]+(\.[0-9]+)?"` and cost ~10–30 ns.

Called 3× per message (bid, ask, last) plus `fast_atoll` for expiry and strike.

**Saving: ~600–1500 ns per message**

---

### 3. Fast integer-to-string (`fast_u64toa`)
**Where:** `ThetaDataClient::processMessage()` — option key building

Replaced `snprintf("%s%lld%c%lld", ...)` with a manual digit writer. `snprintf` invokes the C runtime formatter and touches locale state. The manual builder costs ~10 ns vs ~80–150 ns for snprintf.

**Saving: ~70–140 ns per option quote**

---

### 4. Pre-computed `_isCall` bool
**Where:** `TradingEngine::armContract()` → `TradingEngine::processPrice()`

Replaced `_optionContract.right == "C"` (a `std::string` comparison, potential heap read) with a plain `bool _isCall` cached at arm time. The `Contract` struct is ~300–400 bytes; touching it on every tick risks a cache miss.

**Saving: ~5–100 ns per tick (5 ns if cache warm, 100 ns if cold)**

---

### 5. Cached strike price (`_armedStrike`)
**Where:** `TradingEngine::armContract()` → `TradingEngine::processPrice()`

Same problem as `_isCall`: `_optionContract.strike` sits inside a large `Contract` struct full of `std::string` fields. `double _armedStrike` is stored on the same cache line as `_isCall` and other hot state.

**Saving: ~5–100 ns per tick**

---

### 6. Mode 2 spinlock pre-filter
**Where:** `TradingEngine::processPrice()` — before `SpinLockGuard`

In Mode 2, most ticks are redundant — the stock price hasn't moved enough to change the order price. The pre-filter `if (|stockBid - _lastProcessedBid| < spotSens) return` is a single branch on a thread-local value, evaluated *before* the spinlock is acquired. Eliminates the spinlock acquisition on ~85–90% of ticks.

`_lastProcessedBid` is written by the same thread that reads it, so no atomic or lock is needed.

**Saving: ~100–300 ns on filtered ticks (eliminates spinlock acquisition entirely)**

---

### 7. `_contractsSet` atomic pre-filter
**Where:** `TradingEngine::processPrice()` — first check, before everything else

During chain loading (no contract armed), hundreds of quotes per second arrive. Previously each one acquired the spinlock just to check `_contractsSet` and return immediately. Making `_contractsSet` an `atomic<bool>` with `memory_order_acquire` load allows the check to happen before the spinlock, with correct C++ memory ordering guarantees: the `release` store in `armContract()` ensures all contract fields (`_armedStrike`, `_isCall`, order templates) are visible by the time any thread sees `_contractsSet == true`.

**Saving: ~100–200 ns per tick during chain loading phase**

---

### 8. Deferred logging (DeferredLog)
**Where:** `TradingEngine::placeEntryOrder()`, `updateEntryOrder()`, `executeHedgeActual()`

Calling `onLog()` (which does a socket write to ControlServer) while holding a spinlock causes multi-millisecond stalls. The `DeferredLog` pattern fills a stack-allocated `{char msg[128]; bool pending}` struct inside the locked section, then fires the log *after* the spinlock releases.

**Risk eliminated: multi-ms stall on order placement path**

---

### 9. Pre-built Order templates
**Where:** `TradingEngine::armContract()` → `placeEntryOrder()`, `executeHedgeActual()`

`placeOrder()` takes an IBKR `Order` struct with `std::string` fields (`action`, `orderType`, `tif`). Assigning those strings on every order meant 8+ heap operations inside the spinlock or hedge lock. Templates are built once at `armContract()` time; hot-path functions only write `lmtPrice` and `totalQuantity`.

Three templates: `_entryOrderTemplate` (BUY), `_hedgeCallTemplate` (SELL), `_hedgePutTemplate` (BUY). The hedge direction is selected with `_isCall ? _hedgeCallTemplate : _hedgePutTemplate` — one branch, no string write.

**Saving: ~500–800 ns per order placement**

---

### 10. Clock::now() before spinlock
**Where:** `TradingEngine::processPrice()`

`std::chrono::steady_clock::now()` is a syscall (~50–200 ns). It was called inside the spinlock for the `updateDelayMs` check. Moving it before `SpinLockGuard` reduces the critical section by one syscall.

**Saving: ~50–200 ns of spinlock hold time per tick**

---

### 11. Eliminated redundant `getLegalOptionPrice`
**Where:** `placeEntryOrder()`, `updateEntryOrder()`

`processPrice()` computes `targetBid = getLegalOptionPrice(raw)` and passes it to both functions. Internally they were calling `getLegalOptionPrice(price)` again — a redundant round-and-tick-snap. Removed.

**Saving: ~5 ns per order placement**

---

### 12. Cache-line alignment
**Where:** `SpinLock.h`, `TradingEngine.h`

- `SpinLock` uses `alignas(64)` so each lock occupies its own cache line — prevents false sharing between `_processLock` and `_hedgeLock`
- `_stockBid/_stockAsk`, `_optBid/_optAsk`, `_oppOptBid/_oppOptAsk` are each on their own `alignas(64)` cache lines — ThetaData thread writes, IBKR thread reads, no false sharing
- `_entryOrderTemplate`, `_hedgeCallTemplate`, `_hedgePutTemplate` each aligned to 64 bytes
- `ChainRowData` call and put fields on separate cache lines

---

### 13. Transparent hash map lookup
**Where:** `TradingEngine::processIQData()` — `_iqChainMap.find(symbol)`

`_iqChainMap` is `unordered_map<string, ...>`. By default, `find(const char*)` constructs a temporary `std::string`, potentially allocating on the heap. `TransparentStringHash` + `TransparentStringEqual` (with `is_transparent = void`) enable heterogeneous lookup so `find(const char*)` hashes the pointer directly without any allocation.

**Allocation eliminated: 0 heap allocs in processIQData hot path**

---

### 14. Socket tuning
**Where:** `ThetaDataClient::connect()`, `TradingEngine::connectIB()`

All sockets get:
- `TCP_NODELAY` — disables Nagle's algorithm (eliminates up to 40 ms batching delay on small sends)
- `SO_RCVBUF = 1 MB` — large receive buffer prevents kernel drops on burst arrivals
- `SO_SNDBUF = 4 KB` on IBKR trade socket — small send buffer minimises kernel-side queuing latency

Linux-only (via `plat_socket_latency()`):
- `SO_BUSY_POLL = 50 µs` — kernel busy-polls the NIC before sleeping, eliminating the typical 10–50 µs wake-up delay (requires kernel 3.11+)
- `TCP_QUICKACK` — sends TCP ACKs immediately, disabling the 40 ms delayed-ACK timer

---

### 15. Thread scheduling and affinity
**Where:** `engine/platform.h`, `ThetaDataClient::connect()`, `TradingEngine::connectIB()`

| Thread | Core | Windows priority | Linux SCHED_FIFO |
|---|---|---|---|
| ThetaData recv | 3 | TIME_CRITICAL | 99 (highest) |
| IBKR trade reader | 2 | HIGHEST | 80 |
| IBKR info reader | — | ABOVE_NORMAL | 60 |
| ControlServer | — | BELOW_NORMAL | SCHED_OTHER |

The entire process is elevated at startup (`REALTIME_PRIORITY_CLASS` / `SCHED_FIFO 99`). On Linux, `mlockall(MCL_CURRENT | MCL_FUTURE)` locks all memory pages to eliminate page-fault jitter.

Cores 2 and 3 are dedicated to trading threads. Adjust in `platform.h` if your CPU topology differs.

---

### 16. Pre-reserved containers
**Where:** `TradingEngine::TradingEngine()`

All `unordered_map` and `unordered_set` containers that are written during chain loading are `.reserve()`-ed at construction time. This prevents rehashing and reallocation during the trading session.

```cpp
_iqChainMap.reserve(4096);       // ~200 strikes × 2 legs = ~400; 4096 leaves room
_activeIqSymbols.reserve(4096);
_processedExecIds.reserve(256);
chainRows.reserve(2048);
```

---

### Measured latency (approximate, Windows / Ryzen 7 5800X)

| Path segment | Before | After |
|---|---|---|
| JSON parse (full message) | ~2500 ns | ~350 ns |
| processIQData → processPrice (filtered, Mode 2) | ~400 ns | ~80 ns |
| processIQData → placeOrder (unfiltered, armed) | ~18 µs | ~4 µs |
| Option fill → hedge order placement | ~150 µs | ~120 µs |

The dominant remaining cost is the IBKR TCP write in `placeOrder()` (~4–18 µs depending on TWS load). Further reduction would require co-location, kernel bypass (DPDK/RDMA), or a lower-latency broker API.

---

## Graceful shutdown

Press **Ctrl+C** in the engine terminal (or send SIGTERM on Linux). The engine cancels any open entry orders, stops the strategy, and exits cleanly.

---

## Updates

### C# → C++ rewrite: what changed and why

The original version (`PH-V2`) was a C# Windows Forms application — a single process where UI, trading logic, and IBKR callbacks shared the same class (`Form1 : Form, EWrapper`). This section documents the concrete differences between the two versions.

---

#### 1. Architecture — the most important change

**C# (PH-V2):** Single process. UI repainting, order placement, and IBKR callbacks all run in the same application. If the UI is redrawing a 200-strike chain, trading is delayed.

**C++ (RobotTrading):** Two completely separate processes. The engine has no UI dependencies. If the browser freezes, orders still get placed.

---

#### 2. `execDetails` — critical for hedge latency

**C#:**
```csharp
public void execDetails(...) {
    this.Invoke(new Action(() => { ... })); // BLOCKS the IBKR reader thread
```
`this.Invoke` is **synchronous** — the IBKR reader thread waits for the UI thread to be free before processing the fill. If the UI is repainting at that moment, this adds **50–200 ms to hedge latency**.

**C++:** Runs directly on the IBKR reader thread (core 2). No UI, no blocking, no marshaling.

---

#### 3. Lock type on the processing path

**C#:** `lock(_processLock)` = `Monitor.Enter/Exit` = .NET runtime mutex → **~100–300 ns per acquisition**. The `spotSens` pre-filter is **inside** the lock — every single tick acquires it.

**C++:** Cache-line-aligned `SpinLock` with `_mm_pause()` → **~5–10 ns**. Both the `_contractsSet` check and the `spotSens` filter run **before** the spinlock. ~85–90% of ticks never touch the lock at all.

---

#### 4. Heap allocations on the hot path

**C# — every order placement:**
```csharp
Order o = new Order { Action = "BUY", OrderType = "LMT", Tif = "DAY", ... };
// 4+ heap allocations + GC pressure on every call
```

**C++:** `_entryOrderTemplate` is built once at `armContract()` time. On the hot path only `lmtPrice` is updated. Zero heap allocations.

Same for hedge orders — C# allocates a new `Order` with `action = (Right == "C") ? "SELL" : "BUY"` on every hedge. C++ uses two pre-built templates (`_hedgeCallTemplate`, `_hedgePutTemplate`) selected by a single bool branch.

---

#### 5. String comparisons on every tick

**C#:**
```csharp
double intrinsic = (OptionContract.Right == "C") // heap string compare every tick
    ? Math.Max(0, mid - OptionContract.Strike)
    : Math.Max(0, OptionContract.Strike - mid);
```

**C++:** `bool _isCall` and `double _armedStrike` are pre-cached at `armContract()` time on the same cache line as other hot state. One branch on a local bool, one read of a local double.

---

#### 6. Market data parsing

**C# (IQFeed):** Uses `IQFeed.CSharpApiClient` — a managed library that parses IQFeed's proprietary text protocol, allocates managed objects per message, and passes data through multiple abstraction layers. Symbol format: `SPY241220C500` (month encoded as letter: A=Jan call, M=Jan put).

**C++ (ThetaData):** Custom WebSocket client with a single-pass JSON parser. All fields extracted in one scan. Hand-rolled `fast_atof` / `fast_atoll` (~10–30 ns each vs ~100–300 ns for MSVC `atof`). Symbol format: `SPY20241220C500000` (OSI standard, strike in millicents).

---

#### 7. `DateTime.Now` on every tick

**C#:** `_lastIqDataTime = DateTime.Now` on **every** IQFeed callback + `DateTime.Now - _lastEntryModTime` comparisons inside the lock. `DateTime.Now` is a syscall with allocation overhead.

**C++:** `Clock::now()` (steady_clock) called once, before the spinlock, result reused throughout `processPrice`. Not called at all on filtered ticks.

---

#### 8. Thread priority and CPU affinity

**C#:** Threads created as `IsBackground = true` with no explicit priority or affinity. The OS scheduler can preempt them at any time.

**C++:**
- Entire process: `REALTIME_PRIORITY_CLASS` (Windows) / `SCHED_FIFO 99` (Linux)
- ThetaData recv thread: `TIME_CRITICAL` / `SCHED_FIFO 99`, pinned to core 3
- IBKR trade reader: `HIGHEST` / `SCHED_FIFO 80`, pinned to core 2
- `mlockall(MCL_CURRENT | MCL_FUTURE)` on Linux prevents page-fault jitter

---

#### 9. Exception handling on the hot path

**C#** `ProcessPrice`:
```csharp
lock (_processLock) {
    try { ... }
    catch { }  // non-zero overhead even when no exception is thrown
}
```

**C++:** No exceptions on the hot path. All critical functions are `noexcept`. No `try/catch` overhead.

---

#### 10. Logic — unchanged

The trading logic itself is identical between versions: same entry modes (static price / dynamic discount), same triple-sensor hedge (`execDetails` + `orderStatus` + `updatePortfolio`), same edge calculation, same parameters. Only the execution infrastructure changed.

---

#### Estimated latency comparison

| Operation | C# (PH-V2) | C++ (RobotTrading) |
|---|---|---|
| Lock acquisition (processing path) | ~150 ns | ~7 ns |
| Filtered tick (spotSens) | ~150 ns (enters lock first) | ~15 ns (returns before lock) |
| Market data message parse | ~2500 ns | ~350 ns |
| Order placement (alloc overhead) | ~800 ns extra (GC) | 0 alloc |
| execDetails → evaluateHedge | +UI thread delay (0–200 ms) | direct, ~0 overhead |
| **Tick → placeOrder total** | **~20–50 µs** | **~4–8 µs** |

---

## License

Private. All rights reserved.
