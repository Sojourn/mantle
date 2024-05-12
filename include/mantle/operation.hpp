#pragma once

#include <new>
#include <cassert>

namespace mantle {

    inline Operation::operator bool() const noexcept {
        return tagged_pointer != 0;
    }

    inline const Object* Operation::object() const noexcept {
        uintptr_t pointer = tagged_pointer & POINTER_MASK;

        Object* object;
        memcpy(&object, &pointer, sizeof(object));
        return std::launder(object); // Implicit conversion to `const Object*`.
    }

    inline Object* Operation::mutable_object() const noexcept {
        uintptr_t pointer = tagged_pointer & POINTER_MASK;

        Object* object;
        memcpy(&object, &pointer, sizeof(object));
        return std::launder(object);
    }

    inline OperationType Operation::type() const noexcept {
        return static_cast<OperationType>((tagged_pointer & TYPE_MASK) >> TYPE_SHIFT);
    }

    inline uint8_t Operation::exponent() const noexcept {
        return static_cast<uint8_t>((tagged_pointer & EXPONENT_MASK) >> EXPONENT_SHIFT);
    }

    inline uint8_t Operation::magnitude() const noexcept {
        return 1u << exponent();
    }

    inline int64_t Operation::value() const noexcept {
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

    inline Operation& OperationBatch::operator[](Sequence sequence) {
        return operations[sequence % SIZE];
    }

    inline const Operation& OperationBatch::operator[](Sequence sequence) const {
        return operations[sequence % SIZE];
    }

    constexpr size_t to_index(OperationType type) {
        return static_cast<size_t>(type);
    }

    constexpr std::string_view to_string(OperationType type) {
        using namespace std::literals;

        switch (type) {
            case OperationType::INCREMENT: return "INCREMENT"sv;
            case OperationType::DECREMENT: return "DECREMENT"sv;
        }

        abort();
    }

    inline Operation make_operation(Object* object, OperationType type, uint8_t exponent) {
        assert(exponent <= Operation::EXPONENT_MAX);

        uintptr_t pointer = 0;
        memcpy(&pointer, &object, sizeof(pointer));

        uintptr_t tag = 0;
        tag |= (static_cast<uint8_t>(type) << Operation::TYPE_SHIFT);
        tag |= (static_cast<uint8_t>(exponent) << Operation::EXPONENT_SHIFT);

        return {
            .tagged_pointer = pointer | tag,
        };
    }

    inline Operation make_null_operation() {
        return make_operation(nullptr, OperationType::INCREMENT);
    }

    inline Operation make_increment_operation(Object* object, uint8_t exponent) {
        return make_operation(object, OperationType::INCREMENT, exponent);
    }

    inline Operation make_decrement_operation(Object* object, uint8_t exponent) {
        return make_operation(object, OperationType::DECREMENT, exponent);
    }

    template<typename OperationHandler>
    inline void for_each_operation(const OperationBatch* first, const OperationBatch* last, OperationHandler&& handler) {
        for (const OperationBatch* batch = first; batch != last; ++batch) {
            for (size_t i = 0; i < OperationBatch::SIZE; ++i) {
                handler(batch->operations[i]);
            }
        }
    }

}
