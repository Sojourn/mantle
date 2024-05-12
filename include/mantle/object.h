#pragma once

#include <cstdint>
#include <cstddef>
#include "mantle/types.h"
#include "mantle/operation.h"

namespace mantle {

    // This alignment gives us 4 tag bits to use in the encoding of an operation.
    class alignas(16) Object {
        Object(Object&&) = delete;
        Object(const Object&) = delete;
        Object& operator=(Object&&) = delete;
        Object& operator=(const Object&) = delete;

    public:
        Object(uint16_t user_data = 0);
        ~Object();

        bool is_managed() const;
        RegionId region_id() const;

        // There are two unused bytes that can be used for an unspecified
        // user defined purpose (age, flags, type information, etc).
        uint16_t user_data() const;
        void set_user_data(uint16_t user_data);

    private:
        template<typename T>
        friend class Handle;
        friend class Region;

        // Associate this `Object` to the local `Region`. Reference counting
        // and object finalization will be handled by that `Region. An `Object`
        // can only be bound once, when a handle to it is first created.
        //
        void bind(RegionId region_id);

        // Submit an operation to the `Region` who will forward it to the `Domain`.
        void start_increment_operation(uint8_t exponent);
        void start_increment_operation(Operation operation);
        void start_decrement_operation(uint8_t exponent);
        void start_decrement_operation(Operation operation);

        // Apply an operation, adjusting our reference count.
        template<OperationType type>
        void apply_operation(Operation operation);
        void apply_increment_operation(Operation operation);
        void apply_decrement_operation(Operation operation);

    private:
        uint32_t reference_count_;
        RegionId region_id_;
        uint16_t user_data_;
    };

    template<OperationType type>
    inline void Object::apply_operation(Operation operation) {
        assert(type == operation.type());

        switch (type) {
            case OperationType::INCREMENT: {
                apply_increment_operation(operation);
                break;
            }
            case OperationType::DECREMENT: {
                apply_decrement_operation(operation);
                break;
            }
        }
    }

    // Ensure that we can pack a tag and pointer into an Operation.
    static_assert(alignof(Object) >= (1ull << Operation::TAG_BITS));
    static_assert(sizeof(Object*) == sizeof(Operation));

}
