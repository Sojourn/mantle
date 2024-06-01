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
        Object(ObjectGroup group = 0);
        ~Object();

        bool is_managed() const;
        RegionId region_id() const;
        ObjectGroup group() const;

    private:
        template<typename T>
        friend class Handle;
        friend class Region;
        friend class RegionController;

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
        // These return true if the object is still referenced after.
        template<OperationType type>
        bool apply_operation(Operation operation);
        bool apply_increment_operation(Operation operation);
        bool apply_decrement_operation(Operation operation);

    private:
        uint32_t    reference_count_;
        RegionId    region_id_;
        ObjectGroup group_;
    };

    template<OperationType type>
    bool Object::apply_operation(const Operation operation) {
        assert(type == operation.type());

        switch (type) {
            case OperationType::INCREMENT: {
                return apply_increment_operation(operation);
            }
            case OperationType::DECREMENT: {
                return apply_decrement_operation(operation);
            }
        }
    }

    // Ensure that we can pack a tag and pointer into an Operation.
    static_assert(alignof(Object) >= (1ull << Operation::TAG_BITS));
    static_assert(sizeof(Object*) == sizeof(Operation));

}
