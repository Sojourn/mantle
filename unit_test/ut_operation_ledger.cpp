#include "catch.hpp"
#include "mantle/operation_ledger.h"
#include <cstring>

using namespace mantle;

TEST_CASE("OperationLedger") {
    static constexpr size_t OPERATION_COUNT = 16;
    static constexpr size_t OPERATION_LEDGER_CAPACITY = 1024;

    OperationLedger ledger(OPERATION_LEDGER_CAPACITY);

    Operation operations[OPERATION_COUNT];
    memset(&operations, 0, sizeof(operations));

    for (size_t i = 0; i < OPERATION_COUNT; ++i) {
        Operation& operation = operations[i];

        Object* object;
        memcpy(&object, &operation, sizeof(object)); // U.B.

        OperationType type = ((i % 2) == 0) ? OperationType::INCREMENT: OperationType::DECREMENT;
        uint8_t exponent = i & Operation::EXPONENT_MASK;

        operation = make_operation(object, type, exponent);
    }

    SECTION("Commit empty transactions") {
        for (Sequence i = 0; i < 13; ++i) {
            ledger.begin_transaction();
            CHECK(ledger.commit_transaction() == SequenceRange(0, 0));
        }
    }

    // Ledger capacity should be freed up when transactions are pruned from the log.
    SECTION("Reuse ledger and transaction log entries") {
        Operation operation = operations[0];

        for (size_t i = 0; i < 3; ++i) {
            // Consume all ledger entries in two transactions.
            for (size_t j = 0; j < 2; ++j) {
                ledger.begin_transaction();
                for (Sequence k = 0; k < OPERATION_LEDGER_CAPACITY / 2; ++k) {
                    CHECK(ledger.write(operation) == true);
                }

                CHECK(ledger.commit_transaction().size() == OPERATION_LEDGER_CAPACITY / 2);
            }

            // Roll off the transactions except for the ones we just added.
            for (size_t j = 0; j < (ledger.transaction_log().capacity() - 2); ++j) {
                ledger.begin_transaction();
                CHECK(ledger.commit_transaction().size() == 0);
            }
        }
    }
}
