#pragma once

#include <cstdint>
#include <cstddef>
#include "mantle/types.h"
#include "mantle/operation.h"

namespace mantle {

    // This alignment gives us 3 tag bits to use in the encoding of an operation.
    class alignas(alignof(void*)) Object {
    public:
        explicit Object(ObjectGroup group = 0);
        ~Object();

        Object(Object&&) = delete;
        Object(const Object&) = delete;
        Object& operator=(Object&&) = delete;
        Object& operator=(const Object&) = delete;

        [[nodiscard]]
        bool is_managed() const;

        [[nodiscard]]
        RegionId region_id() const;

        [[nodiscard]]
        ObjectGroup group() const;

    private:
        template<typename T>
        friend class Ref;
        friend class Region;
        friend class RegionController;

        // Associate this `Object` to the local `Region`. Reference counting
        // and object finalization will be handled by that `Region. An `Object`
        // can only be bound once, when a handle to it is first created.
        //
        void bind(RegionId region_id);

        // Update the reference count of this `Object` by the given magnitude.
        // These functions return `true` if the reference count remains positive.
        bool apply_increment(uint32_t delta_magnitude);
        bool apply_decrement(uint32_t delta_magnitude);

    private:
        uint32_t    reference_count_;
        RegionId    region_id_;
        ObjectGroup group_;
    };

    // Ensure that we can pack a tag and pointer into an Operation.
    static_assert(alignof(Object) >= (1ull << Operation::TAG_BITS));
    static_assert(sizeof(Object*) == sizeof(Operation));

}
