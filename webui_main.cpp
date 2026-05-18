// ============================================================
// webui_main.cpp — Web UI process
//
// Connects to engine on :7799, serves browser UI on :8080.
// Start robottrading_engine first, then this.
// Open http://localhost:8080 in browser.
// ============================================================

#include "engine/platform.h"
#include "webui/EngineProxy.h"
#include "webui/HttpServer.h"
#include <cstdio>
#include <csignal>
#include <thread>
#include <chrono>

#ifdef _WIN32
#  include <shellapi.h>
#  pragma comment(lib, "shell32.lib")
static BOOL WINAPI consoleHandler(DWORD) { return TRUE; }
#else
static volatile sig_atomic_t g_stop = 0;
static void sigHandler(int) { g_stop = 1; }
#endif

int main() {
    EngineProxy proxy(7799);
    HttpServer  http(proxy, 8080);

    proxy.setHttpServer(&http);

    if (!http.start()) {
        fprintf(stderr, "[ui] Failed to start HttpServer on :8080\n");
        return 1;
    }
    printf("[ui] Web UI listening on http://localhost:8080\n");

    // Try to connect to engine (retry every 2 s until success)
    while (!proxy.isConnected()) {
        if (proxy.connect("127.0.0.1")) {
            printf("[ui] Connected to engine on :7799\n");
        } else {
            printf("[ui] Waiting for engine on :7799 ...\n");
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    // Open browser automatically (Windows only)
#ifdef _WIN32
    ShellExecuteA(nullptr, "open", "http://localhost:8080", nullptr, nullptr, SW_SHOWNORMAL);
    SetConsoleCtrlHandler(consoleHandler, TRUE);
    while (proxy.isConnected()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
#else
    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[ui] Open http://localhost:8080 in your browser\n");
    while (proxy.isConnected() && !g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
#endif

    printf("[ui] Shutting down.\n");
    http.stop();
    proxy.disconnect();
    return 0;
}
