#pragma once

#include <utility>
#include <compare>
#include <optional>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <cassert>
#include "mantle/util.h"
#include "mantle/types.h"
#include "mantle/object.h"

namespace mantle {

    // TODO: SSE2 probing.
    // TODO: Live mask that we can `&` with the probe result.
    //
    template<typename T, size_t CACHE_SIZE, size_t CACHE_WAYS>
    class ObjectCache {
    public:
        static_assert(is_power_of_2(CACHE_SIZE));
        static_assert(is_power_of_2(CACHE_WAYS));

        static constexpr size_t CACHE_SETS = CACHE_SIZE / CACHE_WAYS;

        static constexpr uintptr_t SET_SHIFT = log2_floor(alignof(Object));
        static constexpr uintptr_t SET_BITS  = log2_floor(CACHE_SETS);
        static constexpr uintptr_t SET_MASK  = (1ull << SET_BITS) - 1;

    public:
        struct Entry {
            Object* key;
            T       val;

            auto operator<=>(const Entry&) const noexcept = default;
        };

        class Cursor {
        public:
            explicit Cursor(size_t pos = 0)
                : pos_(pos)
            {
                assert(pos_ <= CACHE_SIZE);
            }

            explicit Cursor(size_t set, size_t way)
                : Cursor((set * CACHE_WAYS) + way)
            {
                assert(pos_ <= CACHE_SIZE);
            }

            auto operator<=>(const Cursor&) const noexcept = default;

            explicit operator bool() const {
                return pos_ < CACHE_SIZE;
            }

            size_t set() const {
                return pos_ / CACHE_WAYS;
            }

            size_t way() const {
                return pos_ % CACHE_WAYS;
            }

            std::optional<Cursor> next() const {
                if (Cursor cursor = *this; cursor.advance()) {
                    return cursor;
                }

                return std::nullopt;
            }

            bool advance() {
                assert(*this);

                pos_ += 1;
                return static_cast<bool>(*this);
            }

        private:
            size_t pos_;
        };

    public:
        ObjectCache() {
            reset();
        }

        std::pair<Cursor, Cursor> equal_range(Object* key) const {
            size_t set = to_set(key);

            return {
                Cursor(set, 0),
                Cursor(set + 1, 0),
            };
        }

        Entry load(Cursor cursor) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            return {
                .key = keys_[set][way],
                .val = vals_[set][way],
            };
        }

        void store(Cursor cursor, Entry entry) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            keys_[set][way] = entry.key;
            vals_[set][way] = entry.val;
        }

        void reset(Cursor cursor) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            keys_[set][way] = nullptr;
            vals_[set][way] = T{};
        }

        void reset() {
            for (Cursor cursor; cursor; cursor.advance()) {
                reset(cursor);
            }
        }

    private:
        static size_t to_set(Object* key) {
            uintptr_t ptr;
            memcpy(&ptr, &key, sizeof(ptr));
            return (ptr >> SET_SHIFT) & SET_MASK;
        }

    private:
        Object* keys_[CACHE_SETS][CACHE_WAYS];
        T       vals_[CACHE_SETS][CACHE_WAYS];
    };

}
