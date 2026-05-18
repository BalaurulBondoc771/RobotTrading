#pragma once
// ============================================================
// platform.h — cross-platform OS primitives (Windows / Linux)
//
// All Windows/Linux differences live here.
// Every engine source file includes this instead of
// <winsock2.h> or <windows.h> directly.
//
// Thread affinity cores (adjust to your CPU topology):
//   core 2 — IBKR trade reader   (THREAD_PRIORITY_HIGHEST  / SCHED_FIFO 80)
//   core 3 — ThetaData recv loop  (THREAD_PRIORITY_TIME_CRITICAL / SCHED_FIFO 99)
//
// Linux real-time setup (one-time, run as root or via limits.conf):
//   /etc/security/limits.conf — add:
//       <user>  -  rtprio  99
//       <user>  -  memlock unlimited
//   OR run the binary with: sudo nice -n -20 ./robottrading_engine
// ============================================================

#include <thread>

#ifdef _WIN32
// ──────────────────────── Windows ────────────────────────────

#  define WIN32_LEAN_AND_MEAN
#  define NOMINMAX
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  pragma comment(lib, "ws2_32.lib")

   using socket_t = SOCKET;
   static constexpr socket_t INVALID_SOCK = INVALID_SOCKET;
   static constexpr int      SOCK_ERR     = SOCKET_ERROR;

   // Networking lifecycle
   inline void plat_net_init   () noexcept { WSADATA w; WSAStartup(MAKEWORD(2, 2), &w); }
   inline void plat_net_cleanup() noexcept { WSACleanup(); }

   // Socket close / shutdown
   inline void plat_close_socket(socket_t s) noexcept { if (s != INVALID_SOCK) closesocket(s); }
   inline void plat_shutdown    (socket_t s) noexcept { shutdown(s, SD_BOTH); }

   // Linux-only latency options: no-op on Windows.
   inline void plat_socket_latency(socket_t) noexcept {}

   // Process and thread priority
   inline void plat_process_realtime () noexcept
       { SetPriorityClass(GetCurrentProcess(), REALTIME_PRIORITY_CLASS); }
   inline void plat_thread_realtime  (std::thread::native_handle_type h) noexcept
       { SetThreadPriority(h, THREAD_PRIORITY_TIME_CRITICAL); }
   inline void plat_thread_high      (std::thread::native_handle_type h) noexcept
       { SetThreadPriority(h, THREAD_PRIORITY_HIGHEST); }
   inline void plat_thread_above_norm(std::thread::native_handle_type h) noexcept
       { SetThreadPriority(h, THREAD_PRIORITY_ABOVE_NORMAL); }
   inline void plat_thread_below_norm(std::thread::native_handle_type h) noexcept
       { SetThreadPriority(h, THREAD_PRIORITY_BELOW_NORMAL); }

   // Pin a thread to a logical core
   inline void plat_pin_thread(std::thread::native_handle_type h, int core) noexcept
       { SetThreadAffinityMask(h, 1ULL << core); }

#else
// ──────────────────────── Linux / POSIX ──────────────────────

#  include <sys/socket.h>
#  include <sys/mman.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <unistd.h>
#  include <pthread.h>
#  include <sched.h>

   using socket_t = int;
   static constexpr socket_t INVALID_SOCK = -1;
   static constexpr int      SOCK_ERR     = -1;

   inline void plat_net_init   () noexcept {}
   inline void plat_net_cleanup() noexcept {}

   inline void plat_close_socket(socket_t s) noexcept { if (s >= 0) ::close(s); }
   inline void plat_shutdown    (socket_t s) noexcept { ::shutdown(s, SHUT_RDWR); }

   // SO_BUSY_POLL  — kernel busy-polls the NIC for up to N µs before sleeping,
   //                 eliminating the ~10-50 µs wake-up latency on recv.
   //                 Requires kernel 3.11+. Guard lets it compile on older kernels.
   // TCP_QUICKACK  — sends ACKs immediately (disables 40 ms delayed-ACK timer).
   inline void plat_socket_latency(socket_t s) noexcept {
#  ifdef SO_BUSY_POLL
       int busy = 50;
       setsockopt(s, SOL_SOCKET, SO_BUSY_POLL, &busy, sizeof(busy));
#  endif
       int one = 1;
       setsockopt(s, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
   }

   // SCHED_FIFO process-wide: prevents OS preemption of all threads.
   // Also locks all memory pages to prevent page-fault jitter.
   // Silently no-ops if the process lacks CAP_SYS_NICE.
   inline void plat_process_realtime() noexcept {
       sched_param sp{}; sp.sched_priority = 99;
       sched_setscheduler(0, SCHED_FIFO, &sp);
       mlockall(MCL_CURRENT | MCL_FUTURE);
   }

   // Per-thread SCHED_FIFO priorities.
   // 99 = ThetaData recv (highest — market data must never be preempted)
   // 80 = IBKR trade reader
   // 60 = other trading-relevant threads
   // SCHED_OTHER = background (ControlServer accept/status threads)
   inline void plat_thread_realtime  (std::thread::native_handle_type h) noexcept
       { sched_param sp{}; sp.sched_priority = 99; pthread_setschedparam(h, SCHED_FIFO, &sp); }
   inline void plat_thread_high      (std::thread::native_handle_type h) noexcept
       { sched_param sp{}; sp.sched_priority = 80; pthread_setschedparam(h, SCHED_FIFO, &sp); }
   inline void plat_thread_above_norm(std::thread::native_handle_type h) noexcept
       { sched_param sp{}; sp.sched_priority = 60; pthread_setschedparam(h, SCHED_FIFO, &sp); }
   inline void plat_thread_below_norm(std::thread::native_handle_type h) noexcept
       { (void)h; /* SCHED_OTHER — default Linux scheduling, no change needed */ }

   // Pin a thread to a logical core (requires _GNU_SOURCE for cpu_set_t)
   inline void plat_pin_thread(std::thread::native_handle_type h, int core) noexcept {
       cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(core, &cs);
       pthread_setaffinity_np(h, sizeof(cs), &cs);
   }

#endif  // _WIN32
