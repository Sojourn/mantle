#pragma once

#include <span>
#include <vector>
#include <cstdint>
#include <cstddef>
#include "mantle/object_cache.h"
#include "mantle/operation.h"

namespace mantle {

    struct OperationGrouperMetrics {
        size_t grouped_count           = 0;

        size_t written_count           = 0;
        size_t written_increment_count = 0;
        size_t written_decrement_count = 0;

        size_t flushed_count           = 0;
        size_t flushed_increment_count = 0;
        size_t flushed_decrement_count = 0;
    };

    // This class attempts to reduce the number of random memory writes needed to update reference counts
    // by combining operations on the same object into a single write.
    class OperationGrouper {
        static constexpr size_t CACHE_SIZE = 512;
        static constexpr size_t CACHE_WAYS = 8;

        struct OperationGroup {
            int64_t delta = 0;
            size_t  hit_count = 0;
            size_t  hit_decay = 0;
        };

        using Cache       = ObjectCache<OperationGroup, CACHE_SIZE, CACHE_WAYS>;
        using CacheEntry  = Cache::Entry;
        using CacheCursor = Cache::Cursor;

    public:
        using Metrics = OperationGrouperMetrics;

        OperationGrouper();

        [[nodiscard]]
        const Metrics& metrics() const;

        // Returns true if there are operations missing from the increment/decrement collections
        // because they have yet to be flushed.
        [[nodiscard]]
        bool is_dirty() const;

        [[nodiscard]]
        std::span<std::pair<Object*, int64_t>> increments();

        [[nodiscard]]
        std::span<std::pair<Object*, int64_t>> decrements();

        // Write an operation to the cache. If flush is true, the operation is immediately written to the
        // increment or decrement collection. Otherwise, the operation is grouped with other operations.
        void write(Operation operation, bool flush = false);

        // Flush operations from the cache to the increment and decrement collections.
        // The force parameter will cause all operations to be flushed regardless of the hit count,
        // which is useful during shutdown.
        void flush(bool force = false);

        // Clear the increment and decrement collections.
        void clear();

        // Equivelent to calling flush(true) and then clear().
        void reset();

    private:
        // Select a cache entry for this object using a bunch of heuristics.
        CacheCursor choose_way(Object* object);

        void flush_group(CacheCursor cursor, bool force);
        void reset_group(CacheCursor cursor);

        void note_operation_written(Operation operation);
        void note_operation_flushed(Operation operation);

    private:
        std::vector<std::pair<Object*, int64_t>> increments_;
        std::vector<std::pair<Object*, int64_t>> decrements_;
        size_t                                   cache_size_;
        Metrics                                  metrics_;
        Cache                                    cache_;
    };

}
