#pragma once

#include <limits>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include <fmt/core.h>
#include "mantle/object.h"
#include "mantle/object_cache.h"
#include "mantle/operation.h"
#include "mantle/operation_writer.h"

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

    // This class attempts to group operations acting on the same `Object`
    // together to reduce the number of operations that need to be applied.
    class OperationGrouper {
        static constexpr size_t CACHE_SIZE = 512;
        static constexpr size_t CACHE_WAYS = 8;

        struct OperationGroup {
            int64_t delta = 0;

            // TODO: Additional heuristic inputs.
            // size_t  count = 0;
            // bool    dirty = false;
        };

        using Cache       = ObjectCache<OperationGroup, CACHE_SIZE, CACHE_WAYS>;
        using CacheEntry  = Cache::Entry;
        using CacheCursor = Cache::Cursor;

    public:
        using Metrics = OperationGrouperMetrics;

        OperationGrouper(
            OperationVectorWriter& increment_writer,
            OperationVectorWriter& decrement_writer
        );

        const Metrics& metrics() const;

        // Returns true if there are writes that have yet to be flushed.
        bool is_dirty() const;

        OperationRange increments();
        OperationRange decrements();

        void write(Operation operation, bool flush = false);
        void flush(bool force = false);
        void reset();

    private:
        // Select a cache entry for this object using a bunch of heuristics.
        CacheCursor choose_way(Object* object);

        void flush_group(CacheCursor cursor, bool force);
        void reset_group(CacheCursor cursor);

        void note_operation_written(Operation operation);
        void note_operation_flushed(Operation operation);

    private:
        OperationVectorWriter& increment_writer_;
        OperationVectorWriter& decrement_writer_;
        size_t                 cache_size_;
        Metrics                metrics_;
        Cache                  cache_;
    };

}
