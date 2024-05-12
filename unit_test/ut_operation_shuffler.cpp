#include "catch.hpp"
#include "mantle/operation.h"
#include "mantle/operation_shuffler.h"
#include "mantle/debug.h"
#include <cstring>
#include <random>
#include <algorithm>

using namespace mantle;

TEST_CASE("OperationShuffler") {
    std::array<Object, 1024> objects;

    auto object_at = [&](const std::vector<OperationBatch>& array, size_t batch_idx, size_t batch_off) {
        return array[batch_idx][batch_off].object();
    };

    auto make_array = [](std::span<Object> objects, OperationType operation_type) {
        OperationVectorWriter writer;
        for (Object& object: objects) {
            writer.write(make_operation(&object, operation_type));
        }

        writer.flush();
        return writer.release();
    };

    auto shuffle = [](std::vector<OperationBatch> array) {
        std::default_random_engine random_engine;
        std::shuffle(array.begin(), array.end(), random_engine);
        return array;
    };

    auto is_sorted = [](const std::vector<OperationBatch>& array) {
        std::optional<Operation> previous_operation;

        for (const OperationBatch& batch: array) {
            for (const Operation& operation: batch.operations) {
                if (previous_operation && *previous_operation > operation) {
                    return false;
                }

                previous_operation = operation;
            }
        }

        return true;
    };

    SECTION("Padding operations") {
        auto array = shuffle(make_array(std::span{objects.data(), 1}, OperationType::INCREMENT));

        OperationShuffler shuffler;
        shuffler.sort(array);
        shuffler.run();

        CHECK(array[0][0].object() == nullptr);
        CHECK(array[0][1].object() == nullptr);
        CHECK(array[0][2].object() == nullptr);
        CHECK(array[0][3].object() == nullptr);
        CHECK(array[0][4].object() == nullptr);
        CHECK(array[0][5].object() == nullptr);
        CHECK(array[0][6].object() == nullptr);
        CHECK(array[0][7].object() == &objects[0]);
    }

    SECTION("Randomized operations") {
        auto increment_array = shuffle(make_array(std::span{objects.data(), objects.size()}, OperationType::INCREMENT));
        auto decrement_array = shuffle(make_array(std::span{objects.data(), objects.size()}, OperationType::DECREMENT));

        OperationShuffler shuffler;
        shuffler.sort(increment_array);
        shuffler.sort(decrement_array);
        shuffler.run();

        CHECK(is_sorted(increment_array));
        CHECK(is_sorted(decrement_array));

        // Check that the lowest-address object is at the front.
        CHECK(object_at(increment_array, 0, 0) == &objects[0]);
        CHECK(object_at(decrement_array, 0, 0) == &objects[0]);

        // Check that the highest-address object is at the front.
        CHECK(object_at(increment_array, increment_array.size() - 1, OperationBatch::SIZE - 1) == &objects[objects.size() - 1]);
        CHECK(object_at(decrement_array, decrement_array.size() - 1, OperationBatch::SIZE - 1) == &objects[objects.size() - 1]);
    }
}
