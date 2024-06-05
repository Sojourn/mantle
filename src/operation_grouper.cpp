#include "mantle/operation_grouper.h"
#include <cassert>

namespace mantle {

    MANTLE_SOURCE_INLINE
    OperationGrouper::OperationGrouper()
        : cache_size_(0)
    {
    }

    MANTLE_SOURCE_INLINE
    auto OperationGrouper::metrics() const -> const Metrics& {
        return metrics_;
    }

    MANTLE_SOURCE_INLINE
    bool OperationGrouper::is_dirty() const {
        return cache_size_ > 0;
    }

    MANTLE_SOURCE_INLINE
    std::span<std::pair<Object*, int64_t>> OperationGrouper::increments() {
        return increments_;
    }

    MANTLE_SOURCE_INLINE
    std::span<std::pair<Object*, int64_t>> OperationGrouper::decrements() {
        return decrements_;
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::write(const Operation operation, const bool flush) {
        Object* object = operation.mutable_object();

        // Ignore no-ops.
        if (UNLIKELY(!object)) {
            return;
        }

        if (flush) {
            // Bypass the cache and immediately flush the operation.
            // The operation doesn't need to be re-encoded which makes this
            // much simpler than flushing an operation group.
            if (operation.type() == OperationType::INCREMENT) {
                increments_.emplace_back(operation.mutable_object(), operation.value());
            }
            else {
                decrements_.emplace_back(operation.mutable_object(), operation.value());
            }
        }
        else {
            CacheCursor cursor = choose_way(object);
            CacheEntry entry = cache_.load(cursor);

            if (entry.key == object) {
                // Update an existing group.
                entry.val.delta += operation.value();
                entry.val.hit_count += 1;
                if (entry.val.delta) {
                    cache_.store(cursor, entry);
                }
                else {
                    cache_.reset(cursor);
                    cache_size_ -= 1;
                }
            }
            else if (entry.key) {
                // Replace an existing group.
                const bool force = true;
                flush_group(cursor, force);

                cache_.store(cursor, CacheEntry {
                    .key = object,
                    .val = {
                        .delta     = operation.value(),
                        .hit_count = 0,
                        .hit_decay = 1,
                    },
                });

                cache_size_ += 1;
            }
            else {
                // Insert a new group.
                cache_.store(cursor, CacheEntry {
                    .key = object,
                    .val = {
                        .delta     = operation.value(),
                        .hit_count = 0,
                        .hit_decay = 1,
                    },
                });

                cache_size_ += 1;
            }

            note_operation_written(operation);
        }
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::flush(const bool force) {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            flush_group(cursor, force);
        }
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::clear() {
        increments_.clear();
        decrements_.clear();
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::reset() {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            reset_group(cursor);
        }
        assert(cache_size_ == 0);

        clear();
    }

    MANTLE_SOURCE_INLINE
    auto OperationGrouper::choose_way(Object* object) -> CacheCursor {
        // Find the set that maps to this object.
        std::pair<CacheCursor, CacheCursor> set = cache_.equal_range(object);

        // Check if an entry for the object already exists in the set.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            if (auto&& [key, _] = cache_.load(cursor); key == object) {
                return cursor;
            }
        }

        // Look for an empty entry.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            if (auto&& [key, _] = cache_.load(cursor); !key) {
                return cursor;
            }
        }

        // Find the entry with the lowest delta magnitude. Break ties by choosing the lowest way.
        {
            CacheCursor min_cursor = set.first;
            int64_t min_delta_magnitude = std::numeric_limits<int64_t>::max();

            for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
                auto&& [key, group] = cache_.load(cursor);

                const int64_t delta = group.delta;
                const int64_t delta_magnitude = delta < 0 ? -delta : +delta;

                if (delta_magnitude < min_delta_magnitude) {
                    min_cursor = cursor;
                    min_delta_magnitude = delta_magnitude;
                }
            }

            return min_cursor;
        }
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::flush_group(const CacheCursor cursor, const bool force) {
        auto&& [key, group] = cache_.load(cursor);
        if (!key) {
            return;
        }

        // Operation groups need an exponential number of hits to avoid being flushed.
        group.hit_decay *= 2;
        if (group.hit_decay < group.hit_count && !force) {
            return; // Seems active, keep this group alive for now.
        }

        std::vector<std::pair<Object*, int64_t>>& collection = group.delta >= 0 ? increments_ : decrements_;
        collection.emplace_back(key, group.delta);

        reset_group(cursor);
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::reset_group(const CacheCursor cursor) {
        if (auto&& [key, _] = cache_.load(cursor); key) {
            assert(cache_size_ > 0);
            cache_.reset(cursor);
            cache_size_ -= 1;
        }
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::note_operation_written(const Operation operation) {
        metrics_.written_count += 1;

        switch (operation.type()) {
            case OperationType::INCREMENT: {
                metrics_.written_increment_count += 1;
                break;
            }
            case OperationType::DECREMENT: {
                metrics_.written_decrement_count += 1;
                break;
            }
        }
    }

    MANTLE_SOURCE_INLINE
    void OperationGrouper::note_operation_flushed(const Operation operation) {
        metrics_.flushed_count += 1;

        switch (operation.type()) {
            case OperationType::INCREMENT: {
                metrics_.flushed_increment_count += 1;
                break;
            }
            case OperationType::DECREMENT: {
                metrics_.flushed_decrement_count += 1;
                break;
            }
        }
    }

}
