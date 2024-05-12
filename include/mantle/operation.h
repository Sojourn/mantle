#pragma once

#include <span>
#include <compare>
#include <cstdint>
#include <cstddef>
#include <climits>
#include <cstring>
#include <cassert>
#include "mantle/util.h"
#include "mantle/types.h"
#include "mantle/config.h"

namespace mantle {

    class Object;

    enum class OperationType : uint8_t {
        INCREMENT,
        DECREMENT,
    };

    constexpr size_t OPERATION_TYPE_COUNT = 2;

    constexpr size_t to_index(OperationType type);
    constexpr std::string_view to_string(OperationType type);

    struct Operation {
        // Lower 3 bit of the tag encode an exponent which is used for greater range.
        // This is also a useful optimization for weighted references which are usually
        // split on powers-of-two.
        static constexpr uintptr_t EXPONENT_BITS  = 3;
        static constexpr uintptr_t EXPONENT_SHIFT = 0;
        static constexpr uintptr_t EXPONENT_MASK  = ((1ull << EXPONENT_BITS) - 1) << EXPONENT_SHIFT;
        static constexpr uintptr_t EXPONENT_MIN   = 0;
        static constexpr uintptr_t EXPONENT_MAX   = EXPONENT_MASK;

        // Upper bit of the tag encodes the type (sign).
        static constexpr uintptr_t TYPE_BITS  = 1;
        static constexpr uintptr_t TYPE_SHIFT = EXPONENT_SHIFT + EXPONENT_BITS;
        static constexpr uintptr_t TYPE_MASK  = ((1ull << TYPE_BITS) - 1) << TYPE_SHIFT;

        static constexpr uintptr_t TAG_BITS = EXPONENT_BITS + TYPE_BITS;
        static constexpr uintptr_t TAG_MASK = (1ull << TAG_BITS) - 1;

        static constexpr uintptr_t POINTER_BITS = (sizeof(Object*) * CHAR_BIT) - TAG_BITS;
        static constexpr uintptr_t POINTER_MASK = ~TAG_MASK;

        uintptr_t tagged_pointer;

        explicit operator bool() const noexcept;
        auto operator<=>(const Operation&) const noexcept = default;

        const Object* object() const noexcept;
        Object* mutable_object() const noexcept;

        OperationType type() const noexcept;
        uint8_t exponent() const noexcept;
        uint8_t magnitude() const noexcept;
        int64_t value() const noexcept;
    };

    // NOTE: `capacity == size` since it is always padded by null operations.
    struct alignas(64) OperationBatch {
        static constexpr size_t SIZE  = CACHE_LINE_SIZE / sizeof(Operation);
        static constexpr size_t SHIFT = log2_floor(SIZE);
        static constexpr size_t MASK  = SIZE - 1; // TODO: Remove this.

        Operation operations[SIZE];

        Operation& operator[](Sequence sequence);
        const Operation& operator[](Sequence sequence) const;
    };

    struct OperationRange {
        OperationBatch* head;
        OperationBatch* tail;
    };

    Operation make_operation(Object* object, OperationType type, uint8_t exponent = 0);

    Operation make_null_operation();

    Operation make_increment_operation(Object* object, uint8_t exponent = 0);
    Operation make_decrement_operation(Object* object, uint8_t exponent = 0);

    template<typename OperationHandler>
    void for_each_operation(const OperationBatch* first, const OperationBatch* last, OperationHandler&& handler);

    static_assert(std::is_trivial_v<Operation>, "Operation must be a trivial type.");

}

#include "operation.hpp"
