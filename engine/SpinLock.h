#pragma once
#include <atomic>
#ifdef _MSC_VER
#  include <intrin.h>
#  define HFT_PAUSE() _mm_pause()
#else
#  include <immintrin.h>
#  define HFT_PAUSE() __builtin_ia32_pause()
#endif

// Cache-line isolated spinlock — prevents false sharing.
// Stays locked only for microseconds; faster than std::mutex for short critical sections.
class SpinLock {
    alignas(64) std::atomic_flag _flag = ATOMIC_FLAG_INIT;
    char _pad[63]; // fill cache line so adjacent spinlocks don't share a cache line
public:
    SpinLock() = default;
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    void lock() noexcept {
        while (_flag.test_and_set(std::memory_order_acquire))
            HFT_PAUSE(); // signals CPU to reduce power/memory contention while spinning
    }
    void unlock() noexcept {
        _flag.clear(std::memory_order_release);
    }
    bool try_lock() noexcept {
        return !_flag.test_and_set(std::memory_order_acquire);
    }
};

struct SpinLockGuard {
    SpinLock& _l;
    explicit SpinLockGuard(SpinLock& l) noexcept : _l(l) { _l.lock(); }
    ~SpinLockGuard() noexcept { _l.unlock(); }
    SpinLockGuard(const SpinLockGuard&) = delete;
};
