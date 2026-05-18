// ============================================================
// main.cpp — Engine process entry point
//
// Starts TradingEngine + ControlServer on :7799.
// All control comes via TCP from robottrading_ui.
// Run the UI process separately, then open http://localhost:8080
//
// Linux real-time setup (one-time — see engine/platform.h):
//   /etc/security/limits.conf:
//       <user>  -  rtprio   99
//       <user>  -  memlock  unlimited
// ============================================================

#include "engine/platform.h"
#include "engine/TradingEngine.h"
#include "engine/ControlServer.h"
#include <cstdio>
#include <csignal>
#include <chrono>
#include <thread>

// Shared stop flag — written by signal/Ctrl+C handler, read by main loop.
// volatile sig_atomic_t is the only async-signal-safe type for signal handlers.
static volatile sig_atomic_t g_stop = 0;

#ifdef _WIN32
static BOOL WINAPI consoleHandler(DWORD) { g_stop = 1; return TRUE; }
#else
static void sigHandler(int) { g_stop = 1; }
#endif

int main() {
    // Elevate the entire process to real-time scheduling so the OS never
    // preempts the trading threads.  On Linux, requires CAP_SYS_NICE or
    // an appropriate /etc/security/limits.conf rtprio entry; silently
    // continues if the privilege is absent.
    plat_process_realtime();

    // Install Ctrl+C / SIGTERM handler for graceful shutdown.
#ifdef _WIN32
    SetConsoleCtrlHandler(consoleHandler, TRUE);
#else
    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);
#endif

    TradingEngine engine;
    ControlServer ctrl(engine, 7799);

    // Wire engine log → ControlServer broadcast
    engine.onLog = [&ctrl](const char* msg) {
        ctrl.broadcastLog(msg);
    };

    // Default parameters (UI can change these at runtime via setparam command)
    engine.params.entryMode     = 2;
    engine.params.staticPrice   = 5.0;
    engine.params.dynDiscount   = 0.10;
    engine.params.updateDelayMs = 2000;
    engine.params.spotSens      = 0.05;
    engine.params.hedgeOffset   = 0.05;
    engine.params.optQty        = 1.0;
    engine.params.hedgeQty      = 100.0;

    if (!ctrl.start()) {
        fprintf(stderr, "[engine] Failed to start ControlServer on :7799\n");
        return 1;
    }

    printf("[engine] ControlServer listening on :7799\n");
    printf("[engine] Start robottrading_ui and open http://localhost:8080\n");
    fflush(stdout);

    while (ctrl.isRunning() && !g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    engine.disconnectIQ();
    engine.disconnectIB();
    ctrl.stop();

    printf("[engine] Shutdown complete.\n");
    return 0;
}
