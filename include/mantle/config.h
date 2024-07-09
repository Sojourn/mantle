#pragma once

#include <span>
#include <optional>
#include <cstddef>

// TODO: Do platform detection, this is lazy.
#ifndef MANTLE_CACHE_LINE_SIZE
#  define MANTLE_CACHE_LINE_SIZE 64
#endif

#ifndef MANTLE_ENABLE_OBJECT_GROUPING
#  define MANTLE_ENABLE_OBJECT_GROUPING true
#endif

// The number of messages that can be queued between `Domain` and `Region` endpoints.
#ifndef MANTLE_STREAM_CAPACITY
#  define MANTLE_STREAM_CAPACITY 4096
#endif

// This trades off memory usage for a reduced number of write protection faults
// that need to be handled to extend write barriers.
// Ideally this is sized such that write protection faults never happen in steady state.
#ifndef MANTLE_WRITE_BARRIER_SEGMENT_CAPACITY
#  define MANTLE_WRITE_BARRIER_SEGMENT_CAPACITY (16 * 1024)
#endif

namespace mantle {

    // TODO: Remove this.
    struct Config {
        std::optional<std::span<size_t>> domain_cpu_affinity;

        // The maximum number of pending operations per-region.
        size_t ledger_capacity = 1024 * 1024;

        // This enables the grouper which tries to consolidate operations on the same object.
        // and net their effects to reduce the number of operations that need to be retired/applied.
        bool operation_grouper_enabled = true;
    };

}
