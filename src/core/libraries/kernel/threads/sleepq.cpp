// SPDX-FileCopyrightText: Copyright 2024 shadPS4 Emulator Project
// SPDX-License-Identifier: GPL-2.0-or-later

#include <array>
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <string_view>
#include "common/spin_lock.h"
#include "core/libraries/kernel/threads/pthread.h"
#include "core/libraries/kernel/threads/sleepq.h"

namespace Libraries::Kernel {

enum class SleepQueueLockMode {
    Spin,
    Mutex,
    Hybrid,
};

static SleepQueueLockMode GetSleepQueueLockMode() {
    static const SleepQueueLockMode mode = [] {
        if (const char* value = std::getenv("SHADPS4_SLEEPQ_LOCK")) {
            if (std::string_view{value} == "mutex") {
                return SleepQueueLockMode::Mutex;
            }
            if (std::string_view{value} == "hybrid") {
                return SleepQueueLockMode::Hybrid;
            }
        }

        // Keep the first experiment's boolean switch working for reproducible old runs.
        const char* use_host_mutex = std::getenv("SHADPS4_SLEEPQ_USE_MUTEX");
        if (use_host_mutex != nullptr && use_host_mutex[0] != '\0' &&
            use_host_mutex[0] != '0') {
            return SleepQueueLockMode::Mutex;
        }
        return SleepQueueLockMode::Spin;
    }();
    return mode;
}

class HybridSpinLock {
public:
    void lock() {
        // Sleep-queue critical sections are normally short. Keep their fast path local, but stop
        // burning a host core if the owner was descheduled or the queue is heavily contended.
        constexpr u32 SpinLimit = 256;
        for (u32 attempt = 0; attempt < SpinLimit; ++attempt) {
            if (!locked.test(std::memory_order_relaxed) &&
                !locked.test_and_set(std::memory_order_acquire)) {
                return;
            }
        }
        while (locked.test_and_set(std::memory_order_acquire)) {
            locked.wait(true, std::memory_order_relaxed);
        }
    }

    void unlock() {
        locked.clear(std::memory_order_release);
        locked.notify_one();
    }

private:
    std::atomic_flag locked = ATOMIC_FLAG_INIT;
};

static constexpr int HASHSHIFT = 9;
static constexpr int HASHSIZE = (1 << HASHSHIFT);
#define SC_HASH(wchan)                                                                             \
    ((u32)((((uintptr_t)(wchan) >> 3) ^ ((uintptr_t)(wchan) >> (HASHSHIFT + 3))) & (HASHSIZE - 1)))
#define SC_LOOKUP(wc) &sc_table[SC_HASH(wc)]

struct SleepQueueChain {
    void Lock() {
        switch (GetSleepQueueLockMode()) {
        case SleepQueueLockMode::Mutex:
            mutex.lock();
            break;
        case SleepQueueLockMode::Hybrid:
            hybrid_lock.lock();
            break;
        case SleepQueueLockMode::Spin:
            spin_lock.lock();
            break;
        }
    }

    void Unlock() {
        switch (GetSleepQueueLockMode()) {
        case SleepQueueLockMode::Mutex:
            mutex.unlock();
            break;
        case SleepQueueLockMode::Hybrid:
            hybrid_lock.unlock();
            break;
        case SleepQueueLockMode::Spin:
            spin_lock.unlock();
            break;
        }
    }

    Common::SpinLock spin_lock;
    std::mutex mutex;
    HybridSpinLock hybrid_lock;
    SleepqList sc_queues;
    int sc_type;
};

static std::array<SleepQueueChain, HASHSIZE> sc_table{};

void SleepqLock(void* wchan) {
    if (g_curthread != nullptr) {
        g_curthread->locklevel.fetch_add(1, std::memory_order_acq_rel);
    }
    SleepQueueChain* sc = SC_LOOKUP(wchan);
    sc->Lock();
}

void SleepqUnlock(void* wchan) {
    SleepQueueChain* sc = SC_LOOKUP(wchan);
    sc->Unlock();
    if (g_curthread != nullptr) {
        const int previous = g_curthread->locklevel.fetch_sub(1, std::memory_order_acq_rel);
        ASSERT(previous > 0);
        if (previous == 1) {
            PthreadCancelInterrupt();
        }
    }
}

SleepQueue* SleepqLookup(void* wchan) {
    SleepQueueChain* sc = SC_LOOKUP(wchan);
    for (auto& sq : sc->sc_queues) {
        if (sq.sq_wchan == wchan) {
            return std::addressof(sq);
        }
    }
    return nullptr;
}

void SleepqAdd(void* wchan, Pthread* td) {
    SleepQueue* sq = SleepqLookup(wchan);
    if (sq != nullptr) {
        sq->sq_freeq.push_front(*td->sleepqueue);
    } else {
        SleepQueueChain* sc = SC_LOOKUP(wchan);
        sq = td->sleepqueue;
        sc->sc_queues.push_front(*sq);
        sq->sq_wchan = wchan;
        /* sq->sq_type = type; */
    }
    td->sleepqueue = nullptr;
    td->wchan = wchan;
    // libkernel uses a TAILQ here. Signal therefore selects the oldest waiter.
    sq->sq_blocked.push_back(td);
}

bool SleepqRemove(SleepQueue* sq, Pthread* td) {
    ASSERT_MSG(sq != nullptr, "Cannot remove a thread from a null sleep queue");
    if (sq == nullptr) [[unlikely]] {
        return false;
    }

    const auto removed = std::erase(sq->sq_blocked, td);
    const bool has_waiters = !sq->sq_blocked.empty();
    ASSERT_MSG(removed == 1, "Thread is missing from its sleep queue");
    if (removed == 0) [[unlikely]] {
        return has_waiters;
    }

    td->wchan = nullptr;
    if (!has_waiters) {
        td->sleepqueue = sq;
        sq->unlink();
        return false;
    }

    ASSERT_MSG(!sq->sq_freeq.empty(), "Sleep queue free list is empty while waiters remain");
    td->sleepqueue = std::addressof(sq->sq_freeq.front());
    sq->sq_freeq.pop_front();
    return true;
}

void SleepqDrop(SleepQueue* sq, void (*callback)(Pthread*, void*), void* arg) {
    if (sq->sq_blocked.empty()) {
        return;
    }

    sq->unlink();
    Pthread* td = sq->sq_blocked.front();
    sq->sq_blocked.pop_front();

    callback(td, arg);

    td->sleepqueue = sq;
    td->wchan = nullptr;

    auto sq2 = sq->sq_freeq.begin();
    for (Pthread* td2 : sq->sq_blocked) {
        callback(td2, arg);
        td2->sleepqueue = std::addressof(*sq2);
        td2->wchan = nullptr;
        ++sq2;
    }
    sq->sq_blocked.clear();
    sq->sq_freeq.clear();
}

} // namespace Libraries::Kernel
