#include "catch.hpp"
#include "mantle/operation_grouper.h"
#include <cstring>

using namespace mantle;

TEST_CASE("OperationGrouper") {
    std::array<Object, 16> objects;
    OperationGrouper grouper;

    auto operation_at = [&](const OperationType type, const size_t index) {
        auto operations = (type == OperationType::INCREMENT) ? grouper.increments() : grouper.decrements();
        REQUIRE(index < operations.size());
        return operations[index];
    };

    auto increment_at = [&](const size_t index) {
        return operation_at(OperationType::INCREMENT, index);
    };

    auto decrement_at = [&](const size_t index) {
        return operation_at(OperationType::DECREMENT, index);
    };

    auto operation_count = [&](const OperationType type) {
        return ((type == OperationType::INCREMENT) ? grouper.increments() : grouper.decrements()).size();
    };

    auto increment_count = [&]() {
        return operation_count(OperationType::INCREMENT);
    };

    auto decrement_count = [&]() {
        return operation_count(OperationType::DECREMENT);
    };

    SECTION("Batch Padding") {
        for (uint8_t exponent = 0; exponent <= Operation::EXPONENT_MAX; ++exponent) {
            Operation increment_operation = make_increment_operation(&objects[0], exponent);
            REQUIRE(increment_operation.object() == &objects[0]);
            REQUIRE(increment_operation.type() == OperationType::INCREMENT);
            REQUIRE(increment_operation.exponent() == exponent);

            Operation decrement_operation = make_decrement_operation(&objects[1], exponent);
            REQUIRE(decrement_operation.object() == &objects[1]);
            REQUIRE(decrement_operation.type() == OperationType::DECREMENT);
            REQUIRE(decrement_operation.exponent() == exponent);

            grouper.write(increment_operation);
            grouper.write(decrement_operation);

            grouper.flush(true);
            CHECK(!grouper.is_dirty());

            REQUIRE(increment_count() == 1);
            {
                auto&& [object, delta] = increment_at(0);
                CHECK(object == &objects[0]);
                CHECK(delta == +(1ll << exponent));
            }

            REQUIRE(decrement_count() == 1);
            {
                auto&& [object, delta] = decrement_at(0);
                CHECK(object == &objects[1]);
                CHECK(delta == -(1ll << exponent));
            }

            grouper.clear();
        }
    }

    SECTION("Merging operations") {
        grouper.write(make_increment_operation(&objects[0], 0)); // +1
        grouper.write(make_increment_operation(&objects[0], 1)); // +2
        grouper.write(make_increment_operation(&objects[0], 1)); // +2
        grouper.write(make_decrement_operation(&objects[1], 0)); // -1
        grouper.write(make_decrement_operation(&objects[1], 1)); // -2
        grouper.write(make_decrement_operation(&objects[1], 1)); // -2
        grouper.flush();

        REQUIRE(increment_count() == 1);
        CHECK(increment_at(0).second == +5);

        REQUIRE(decrement_count() == 1);
        CHECK(decrement_at(0).second == -5);

        grouper.clear();
    }
}
