#pragma once

#include <span>
#include <optional>
#include <cstddef>

// TODO: Use macros for the global variables instead of global variables.

namespace mantle {

    // This flag enables weighted reference counting in handles.
    constexpr bool ENABLE_WEIGHTED_REFERENCE_COUNTING = false;
    constexpr bool ENABLE_OBJECT_GROUPING = true;

    // The number of messages that can be queued between `Domain` and `Region` endpoints.
    constexpr size_t STREAM_CAPACITY = 4096;

    // FIXME: Some architectures have cache lines that are 128 bytes. We should detect this.
    constexpr size_t CACHE_LINE_SIZE = 64;

    // This trades off memory usage for a reduced number of write protection faults
    // that need to be handled to extend write barriers.
    // Ideally this is sized such that write protection faults never happen in steady state.
    constexpr size_t WRITE_BARRIER_SEGMENT_CAPACITY = 16 * 1024;

    struct Config {
        std::optional<std::span<size_t>> domain_cpu_affinity;

        // The maximum number of pending operations per-region.
        size_t ledger_capacity = 1024 * 1024;

        // This enables the grouper which tries to consolidate operations on the same object.
        // and net their effects to reduce the number of operations that need to be retired/applied.
        bool operation_grouper_enabled = true;
    };
}
