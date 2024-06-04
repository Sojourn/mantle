#pragma once

#include <span>
#include <cstdint>
#include <cstddef>
#include <climits>
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

        auto operator<=>(const Operation&) const noexcept = default;

        [[nodiscard]]
        explicit operator bool() const noexcept {
            return tagged_pointer_ != 0;
        }

        [[nodiscard]]
        const Object* object() const noexcept {
            const uintptr_t pointer = tagged_pointer_ & POINTER_MASK;

            Object* object;
            memcpy(&object, &pointer, sizeof(object));
            return std::launder(object); // Implicit conversion to `const Object*`.
        }

        [[nodiscard]]
        Object* mutable_object() const noexcept {
            const uintptr_t pointer = tagged_pointer_ & POINTER_MASK;

            Object* object;
            memcpy(&object, &pointer, sizeof(object));
            return std::launder(object);
        }

        [[nodiscard]]
        OperationType type() const noexcept {
            return static_cast<OperationType>((tagged_pointer_ & TYPE_MASK) >> TYPE_SHIFT);
        }

        [[nodiscard]]
        uint8_t exponent() const noexcept {
            return static_cast<uint8_t>((tagged_pointer_ & EXPONENT_MASK) >> EXPONENT_SHIFT);
        }

        [[nodiscard]]
        uint8_t magnitude() const noexcept {
            return 1u << exponent();
        }

        [[nodiscard]]
        int64_t value() const noexcept {
            switch (type()) {
                case OperationType::INCREMENT: {
                    return +1 << exponent();
                }
                case OperationType::DECREMENT: {
                    return -1 << exponent();
                }
            }

            abort(); // Unreachable.
        }

        uintptr_t tagged_pointer_;
    };
    static_assert(std::is_trivial_v<Operation>, "Operation must be a trivial type.");

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

    inline Operation& OperationBatch::operator[](const Sequence sequence) {
        return operations[sequence % SIZE];
    }

    inline const Operation& OperationBatch::operator[](const Sequence sequence) const {
        return operations[sequence % SIZE];
    }

    inline Operation make_operation(Object* object, const OperationType type, const uint8_t exponent = 0) {
        assert(exponent <= Operation::EXPONENT_MAX);

        uintptr_t pointer = 0;
        memcpy(&pointer, &object, sizeof(pointer));

        uintptr_t tag = 0;
        tag |= (static_cast<uint8_t>(type) << Operation::TYPE_SHIFT);
        tag |= (static_cast<uint8_t>(exponent) << Operation::EXPONENT_SHIFT);

        return {
            .tagged_pointer_ = pointer | tag,
        };
    }

    inline Operation make_null_operation() {
        return make_operation(nullptr, OperationType::INCREMENT);
    }

    inline Operation make_increment_operation(Object* object, const uint8_t exponent = 0) {
        return make_operation(object, OperationType::INCREMENT, exponent);
    }

    inline Operation make_decrement_operation(Object* object, const uint8_t exponent = 0) {
        return make_operation(object, OperationType::DECREMENT, exponent);
    }

    template<typename OperationHandler>
    void for_each_operation(const OperationBatch* first, const OperationBatch* last, OperationHandler&& handler) {
        for (const OperationBatch* batch = first; batch != last; ++batch) {
            for (const Operation& operation: batch->operations) {
                handler(operation);
            }
        }
    }

    constexpr size_t to_index(OperationType type) {
        return static_cast<size_t>(type);
    }

    constexpr std::string_view to_string(const OperationType type) {
        using namespace std::literals;

        switch (type) {
            case OperationType::INCREMENT: return "INCREMENT"sv;
            case OperationType::DECREMENT: return "DECREMENT"sv;
        }

        abort();
    }

}
