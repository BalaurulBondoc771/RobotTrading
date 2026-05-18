// ============================================================
// webui_main.cpp — Web UI process
//
// Connects to engine on :7799, serves browser UI on :8080.
// Start robottrading_engine.exe first, then this.
// Open http://localhost:8080 in browser.
// ============================================================

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <shellapi.h>
#pragma comment(lib, "shell32.lib")
#include <cstdio>
#include <thread>
#include <chrono>

#include "webui/EngineProxy.h"
#include "webui/HttpServer.h"

int main() {
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);

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

    // Open browser automatically
    ShellExecuteA(nullptr, "open", "http://localhost:8080", nullptr, nullptr, SW_SHOWNORMAL);

    // Block until engine disconnects or Ctrl+C
    SetConsoleCtrlHandler([](DWORD) -> BOOL { return TRUE; }, TRUE);
    while (proxy.isConnected()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    printf("[ui] Engine disconnected. Shutting down.\n");

    http.stop();
    proxy.disconnect();
    WSACleanup();
    return 0;
}
