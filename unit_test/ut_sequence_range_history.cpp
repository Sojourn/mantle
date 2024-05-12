#include "catch.hpp"
#include "mantle/operation_ledger.h"
#include <cstring>

using namespace mantle;

TEST_CASE("OperationTransactionLog") {
    SequenceRangeHistory log(4);

    // Check the initial state of the log. We expect a few fake transactions before the wrap.
    {
        CHECK(log.select(0) == SequenceRange(0, 0));
        CHECK(log.select(1) == SequenceRange(0, 0));
        CHECK(log.select(2) == SequenceRange(0, 0));
    }

    // Filling log (1/4).
    {
        log.insert(1);

        CHECK(log.select(0) == SequenceRange(0, 1));
        CHECK(log.select(1) == SequenceRange(0, 0));
        CHECK(log.select(2) == SequenceRange(0, 0));
    }

    // Filling log (2/4).
    {
        log.insert(2);

        CHECK(log.select(0) == SequenceRange(1, 2));
        CHECK(log.select(1) == SequenceRange(0, 1));
        CHECK(log.select(2) == SequenceRange(0, 0));
    }

    // Filling log (3/4).
    {
        log.insert(3);

        CHECK(log.select(0) == SequenceRange(2, 3));
        CHECK(log.select(1) == SequenceRange(1, 2));
        CHECK(log.select(2) == SequenceRange(0, 1));
    }

    // Filling log (4/4).
    {
        log.insert(4);

        CHECK(log.select(0) == SequenceRange(3, 4));
        CHECK(log.select(1) == SequenceRange(2, 3));
        CHECK(log.select(2) == SequenceRange(1, 2));
    }

    // Check overwriting an existing entry in the log.
    {
        log.insert(5);

        CHECK(log.select(0) == SequenceRange(4, 5));
        CHECK(log.select(1) == SequenceRange(3, 4));
        CHECK(log.select(2) == SequenceRange(2, 3));
    }
}
