#include "catch.hpp"
#include "mantle/mantle.h"
#include "mantle/operation.h"

using namespace mantle;

namespace {

    union Pointer {
        uintptr_t as_number;
        Object* as_object;
    };

}

TEST_CASE("Operation") {
    Pointer pointer;
    pointer.as_number = std::numeric_limits<uintptr_t>::max() << 16; // U.B.

    SECTION("General") {
        for (uint8_t i = 0; i <= Operation::EXPONENT_MAX; ++i) {
            Operation increment_operation = make_operation(pointer.as_object, OperationType::INCREMENT, i);
            CHECK(increment_operation.object() == pointer.as_object);
            CHECK(increment_operation.type() == OperationType::INCREMENT);
            CHECK(increment_operation.magnitude() == (1ull << i));
            CHECK(increment_operation.value() == (+1 << i));

            Operation decrement_operation = make_operation(pointer.as_object, OperationType::DECREMENT, i);
            CHECK(decrement_operation.object() == pointer.as_object);
            CHECK(decrement_operation.type() == OperationType::DECREMENT);
            CHECK(decrement_operation.magnitude() == (1ull << i));
            CHECK(decrement_operation.value() == (-1 << i));
        }
    }

    SECTION("Increment") {
        Operation operation = make_increment_operation(pointer.as_object);
        CHECK(operation.object() == pointer.as_object);
        CHECK(operation.type() == OperationType::INCREMENT);
        CHECK(operation.magnitude() == 1);
    }

    SECTION("Decrement") {
        Operation operation = make_decrement_operation(pointer.as_object);
        CHECK(operation.object() == pointer.as_object);
        CHECK(operation.type() == OperationType::DECREMENT);
        CHECK(operation.magnitude() == 1);
    }

    SECTION("Null") {
        Operation operation = make_null_operation();
        CHECK(operation.object() == nullptr);
        CHECK(operation.type() == OperationType::INCREMENT);
        CHECK(operation.magnitude() == 1); // Checking that the exponent is zero.
    }
}
