#include "mantle/operation_grouper.h"
#include <cassert>

namespace mantle {

    OperationGrouper::OperationGrouper(
        OperationVectorWriter& increment_writer,
        OperationVectorWriter& decrement_writer
    )
        : increment_writer_(increment_writer)
        , decrement_writer_(decrement_writer)
        , cache_size_(0)
    {
    }

    auto OperationGrouper::metrics() const -> const Metrics& {
        return metrics_;
    }

    bool OperationGrouper::is_dirty() const {
        return cache_size_ > 0;
    }

    OperationRange OperationGrouper::increments() {
        return increment_writer_.data();
    }

    OperationRange OperationGrouper::decrements() {
        return decrement_writer_.data();
    }

    void OperationGrouper::write(Operation operation, bool flush) {
        Object* object = operation.mutable_object();
        if (UNLIKELY(!object)) {
            assert(false); // Unexpected, but not fatal.
            return;
        }

        if (flush) {
            // Bypass the cache and immediately flush the operation.
            // The operation doesn't need to be re-encoded which makes this
            // much simpler than flushing an operation group.
            if (operation.type() == OperationType::INCREMENT) {
                increment_writer_.write(operation);
            }
            else {
                decrement_writer_.write(operation);
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
                bool force = true;
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

    void OperationGrouper::flush(bool force) {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            flush_group(cursor, force);
        }

        increment_writer_.flush();
        decrement_writer_.flush();
    }

    void OperationGrouper::reset() {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            reset_group(cursor);
        }

        assert(cache_size_ == 0);
    }

    auto OperationGrouper::choose_way(Object* object) -> CacheCursor {
        // Find the set that maps to this object.
        std::pair<CacheCursor, CacheCursor> set = cache_.equal_range(object);

        // Check if an entry for the object already exists in the set.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            auto&& [key, group] = cache_.load(cursor);
            if (key == object) {
                return cursor;
            }
        }

        // Look for an empty entry.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            auto&& [key, group] = cache_.load(cursor);
            if (!key) {
                return cursor;
            }
        }

        // Find the entry with the lowest delta magnitude. Break ties by choosing the lowest way.
        {
            CacheCursor min_cursor = set.first;
            int64_t min_delta_magnitude = std::numeric_limits<int64_t>::max();

            for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
                auto&& [key, group] = cache_.load(cursor);

                int64_t delta = group.delta;
                int64_t delta_magnitude = delta < 0 ? -delta : +delta;

                if (delta_magnitude < min_delta_magnitude) {
                    min_cursor = cursor;
                    min_delta_magnitude = delta_magnitude;
                }
            }

            return min_cursor;
        }
    }

    void OperationGrouper::flush_group(CacheCursor cursor, bool force) {
        auto&& [key, group] = cache_.load(cursor);
        if (!key) {
            return;
        }

        // Operation groups need an exponential number of hits to avoid being flushed.
        group.hit_decay *= 2;
        if (!force) {
            if (group.hit_decay < group.hit_count) {
                return; // Seems active, keep this group alive for now.
            }
        }

        OperationVectorWriter* writer = nullptr;
        OperationType type = OperationType::INCREMENT;
        int64_t remainder = 0;

        // Deal with the sign so we can think about the delta as a remainder that can be reduced (magnitude).
        if (group.delta >= 0) {
            writer = &increment_writer_;
            type = OperationType::INCREMENT;
            remainder = +group.delta;
        }
        else {
            writer = &decrement_writer_;
            type = OperationType::DECREMENT;
            remainder = -group.delta;
        }
        assert(remainder >= 0);

        // Flush the group to one of the streams. This will produce O(log(delta)) operations.
        while (remainder) {
            int64_t exponent = std::min(log2_floor(remainder), static_cast<int64_t>(Operation::EXPONENT_MAX));
            assert(exponent >= 0);

            Operation operation = make_operation(key, type, static_cast<uint8_t>(exponent));
            writer->write(operation);
            note_operation_flushed(operation);

            remainder -= 1ll << exponent;
            assert(remainder >= 0);
        }

        reset_group(cursor);
    }

    void OperationGrouper::reset_group(CacheCursor cursor) {
        auto&& [key, _] = cache_.load(cursor);
        if (!key) {
            return;
        }

        assert(cache_size_ > 0);
        cache_.reset(cursor);
        cache_size_ -= 1;
    }

    void OperationGrouper::note_operation_written(Operation operation) {
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

    void OperationGrouper::note_operation_flushed(Operation operation) {
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
