#include "mantle/object.h"
#include "mantle/region.h"
#include "mantle/debug.h"
#include "mantle/util.h"
#include <cassert>

namespace mantle {

    MANTLE_SOURCE_INLINE
    Object::Object(ObjectGroup group)
        : reference_count_(0)
        , region_id_(INVALID_REGION_ID)
        , group_(group)
    {
    }

    MANTLE_SOURCE_INLINE
    Object::~Object() {
#if MANTLE_AUDIT
        bool halting = !has_region();
        bool dropped = reference_count_ == 0;

        assert(halting || dropped);
#endif
    }

    MANTLE_SOURCE_INLINE
    bool Object::is_managed() const {
        return region_id_ != INVALID_REGION_ID;
    }

    MANTLE_SOURCE_INLINE
    RegionId Object::region_id() const {
        return region_id_;
    }

    MANTLE_SOURCE_INLINE
    ObjectGroup Object::group() const {
        return group_;
    }

    MANTLE_SOURCE_INLINE
    void Object::bind(RegionId region_id) {
        if (UNLIKELY(is_managed())) {
            abort(); // Don't bind an object more than once.
        }

        region_id_ = region_id;
    }

    MANTLE_SOURCE_INLINE
    void Object::start_increment_operation(uint8_t exponent) {
        start_increment_operation(make_increment_operation(this, exponent));
    }

    MANTLE_SOURCE_INLINE
    void Object::start_increment_operation(Operation operation) {
        assert(operation.type() == OperationType::INCREMENT);

        info("[object:{}] start increment - exponent:{}", (const void*)this, operation.exponent());

        if (LIKELY(has_region())) {
            get_region().start_increment_operation(*this, operation);
        }
        else {
#if MANTLE_AUDIT
            assert(false); // Leak?
#endif
        }
    }

    MANTLE_SOURCE_INLINE
    void Object::start_decrement_operation(uint8_t exponent) {
        start_decrement_operation(make_decrement_operation(this, exponent));
    }

    MANTLE_SOURCE_INLINE
    void Object::start_decrement_operation(Operation operation) {
        assert(operation.type() == OperationType::DECREMENT);

        info("[object:{}] start decrement - exponent:{}", (const void*)this, operation.exponent());

        if (LIKELY(has_region())) {
            get_region().start_decrement_operation(*this, operation);
        }
        else {
#if MANTLE_AUDIT
            assert(false); // Leak?
#endif
        }
    }

    MANTLE_SOURCE_INLINE
    bool Object::apply_increment(const uint32_t delta_magnitude) {
        reference_count_ += delta_magnitude;
        return true;
    }

    MANTLE_SOURCE_INLINE
    bool Object::apply_decrement(const uint32_t delta_magnitude) {
        if (reference_count_ < delta_magnitude) {
            reference_count_ = 0;
            region_id_ = INVALID_REGION_ID;
            return false;
        }

        reference_count_ -= delta_magnitude;
        return true;
    }

}
