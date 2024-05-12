#include "catch.hpp"
#include "mantle/operation_grouper.h"
#include <cstring>

using namespace mantle;

TEST_CASE("OperationGrouper") {
    std::array<Object, 16> objects;

    OperationVectorWriter increment_writer;
    OperationVectorWriter decrement_writer;
    OperationGrouper grouper(increment_writer, decrement_writer);

    auto operation_at = [&](OperationVectorWriter& writer, size_t batch_idx, size_t batch_off) {
        return writer.data().head[batch_idx].operations[batch_off];
    };

    auto operation_count = [&](OperationVectorWriter& writer) {
        size_t result = 0;

        size_t batch_cnt = writer.span().size();
        for (size_t batch_idx = 0; batch_idx < batch_cnt; ++batch_idx) {
            for (size_t batch_off = 0; batch_off < OperationBatch::SIZE; ++batch_off) {
                if (operation_at(writer, batch_idx, batch_off)) {
                    result += 1;
                }
            }
        }

        return result;
    };

    auto increment_count = [&]() {
        return operation_count(increment_writer);
    };

    auto decrement_count = [&]() {
        return operation_count(decrement_writer);
    };

    auto clear = [&]() {
        increment_writer.clear();
        decrement_writer.clear();
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
            CHECK(operation_at(increment_writer, 0, 0).object() == &objects[0]);
            CHECK(operation_at(increment_writer, 0, 0).exponent() == exponent);
            CHECK(operation_at(increment_writer, 0, 0).type() == OperationType::INCREMENT);
            for (size_t batch_off = 1; batch_off < OperationBatch::SIZE; ++batch_off) {
                CHECK(operation_at(increment_writer, 0, batch_off) == make_null_operation());
            }

            REQUIRE(decrement_count() == 1);
            CHECK(operation_at(decrement_writer, 0, 0).object() == &objects[1]);
            CHECK(operation_at(decrement_writer, 0, 0).exponent() == exponent);
            CHECK(operation_at(decrement_writer, 0, 0).type() == OperationType::DECREMENT);
            for (size_t batch_off = 1; batch_off < OperationBatch::SIZE; ++batch_off) {
                CHECK(operation_at(decrement_writer, 0, batch_off) == make_null_operation());
            }

            clear();
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

        REQUIRE(increment_count() == 2);
        CHECK(operation_at(increment_writer, 0, 0).value() == +4); // (+2) + (+2) -> (+4)
        CHECK(operation_at(increment_writer, 0, 1).value() == +1);

        REQUIRE(decrement_count() == 2);
        CHECK(operation_at(decrement_writer, 0, 0).value() == -4); // (-2) + (-2) -> (-4)
        CHECK(operation_at(decrement_writer, 0, 1).value() == -1);
    }
}
