#include "mantle/object.h"
#include "mantle/region.h"
#include "mantle/debug.h"
#include "mantle/util.h"
#include <cassert>

namespace mantle {

    Object::Object(ObjectGroup group)
        : reference_count_(0)
        , region_id_(INVALID_REGION_ID)
        , group_(group)
    {
    }

    Object::~Object() {
#if MANTLE_AUDIT
        bool halting = !has_region();
        bool dropped = reference_count_ == 0;

        assert(halting || dropped);
#endif
    }

    bool Object::is_managed() const {
        return region_id_ != INVALID_REGION_ID;
    }

    RegionId Object::region_id() const {
        return region_id_;
    }

    ObjectGroup Object::group() const {
        return group_;
    }

    void Object::bind(RegionId region_id) {
        if (UNLIKELY(is_managed())) {
            abort(); // Don't bind an object more than once.
        }

        region_id_ = region_id;
    }

    void Object::start_increment_operation(uint8_t exponent) {
        start_increment_operation(make_increment_operation(this, exponent));
    }

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

    void Object::start_decrement_operation(uint8_t exponent) {
        start_decrement_operation(make_decrement_operation(this, exponent));
    }

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

    bool Object::apply_increment_operation(Operation operation) {
        assert(operation.object() == this);
        assert(operation.type() == OperationType::INCREMENT);

        info("[object:{}] apply increment - refs:{} exponent:{}", (const void*)this, reference_count_, operation.exponent());

        reference_count_ += operation.magnitude();
        return true;
    }

    bool Object::apply_decrement_operation(Operation operation) {
        assert(operation.object() == this);
        assert(operation.type() == OperationType::DECREMENT);

        info("[object:{}] apply decrement - refs:{} exponent:{}", (const void*)this, reference_count_, operation.exponent());

        uint8_t magnitude = operation.magnitude();
        if (reference_count_ < magnitude) {
            reference_count_ = 0;
            region_id_       = INVALID_REGION_ID;
            return false;
        }
        else   {
            reference_count_ -= magnitude;
            return true;
        }
    }

}
