#pragma once

#include <span>
#include <optional>
#include <cstdint>
#include <cstddef>

namespace mantle {

    // The number of messages that can be queued between `Domain` and `Region` endpoints.
    constexpr size_t STREAM_CAPACITY = 4096;


    // FIXME: Some architectures have cache lines that are 128 bytes. We should detect this.
    constexpr size_t CACHE_LINE_SIZE = 64;

    struct Config {
        std::optional<std::span<size_t>> domain_cpu_affinity;

        // The maximum number of pending operations per-region.
        size_t ledger_capacity = 1024 * 1024;

        // This enables the deflator which tries to group operations on the same object
        // and net their effects to reduce the number of operations that need to be retired/applied.
        bool operation_grouper_enabled = true;

        // These control how aggressively operations are sorted before being applied.
        // It trades CPU cycles for better spacial locality (which has diminishing returns).
        bool operation_shuffler_enabled = true;
        size_t shuffle_max_step_count = 64;
        size_t shuffle_max_depth = 8;
        size_t shuffle_min_partition_size = 20;
    };

}
