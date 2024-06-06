#pragma once

#include <span>
#include <thread>
#include <stdexcept>
#include <cstring>
#include <cstddef>
#include <cassert>
#include <sched.h>

#ifdef __GNUC__
#  define MANTLE_HOT __attribute__((hot, always_inline)) inline
#  define MANTLE_COLD __attribute__((noinline))
#else
#  define MANTLE_HOT inline
#  define MANTLE_COLD inline
#endif

#ifndef LIKELY 
#  define LIKELY(x)   __builtin_expect(!!(x), 1)
#endif

#ifndef UNLIKELY
#  define UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

namespace mantle {

    template<typename T>
    constexpr bool is_power_of_2(T value) {
        return value && (!(value & (value - 1)));
    }

    template<typename T>
    constexpr T log2_floor(T value) {
        if (value == 1) {
            return 0;
        }

        return log2_floor(value >> 1) + 1;
    }

    template<typename T>
    constexpr T log2_ceil(T value) {
        if (value == 1) {
            return 0;
        }

        return log2_floor(value - 1) + 1;
    }

    inline void set_cpu_affinity(std::span<size_t> cpus) {
        cpu_set_t set;
        CPU_ZERO(&set);

        for (const size_t cpu: cpus) {
            CPU_SET(static_cast<int>(cpu), &set);
        }

        if (sched_setaffinity(0, sizeof(set), &set) < 0) {
            throw std::runtime_error("Failed to set cpu affinity");
        }

        std::this_thread::yield();
    }

}
