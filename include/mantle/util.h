#pragma once

#include <span>
#include <array>
#include <thread>
#include <stdexcept>
#include <cstring>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include <fmt/core.h>
#include <sched.h>

#ifdef __GNUC__
#  define MANTLE_HOT __attribute__((hot, always_inline)) inline
#else
#  define MANTLE_HOT inline
#endif

#define MANTLE_COLD

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
            throw std::runtime_error(fmt::format("Failed to set cpu affinity - {}", strerror(errno)));
        }

        std::this_thread::yield();
    }

}
