#pragma once

#include <memory>
#include <cstdint>
#include <cstddef>
#include "mantle/types.h"
#include "mantle/config.h"
#include "mantle/ring.h"
#include "mantle/operation.h"
#include "mantle/operation_writer.h"

namespace mantle {

    class SequenceRangeHistory {
    public:
        SequenceRangeHistory(size_t capacity)
            : next_slot_(0)
            , data_(capacity)
        {
            data_.fill(0);
        }

        size_t capacity() const {
            return data_.size();
        }

        SequenceRange select(int age = 0) const {
            Sequence prev_slot = next_slot_ - 1;

            return {
                .head = data_[prev_slot - age - 1],
                .tail = data_[prev_slot - age - 0],
            };
        }

        // NOTE: The `head` of the new range is implicitly the `tail` of the
        //       previously inserted `SequenceRange`.
        void insert(Sequence tail) {
            data_[next_slot_++] = tail;
        }

    private:
        Sequence       next_slot_;
        Ring<Sequence> data_;
    };

    class OperationLedger {
    public:
        explicit OperationLedger(size_t ledger_capacity)
            : storage_(ledger_capacity)
            , transaction_log_(TRANSACTION_LOG_HISTORY)
            , transaction_head_(0)
            , transaction_tail_(storage_.size())
            , writer_(storage_, transaction_head_, transaction_tail_)
        {
        }

        const SequenceRangeHistory& transaction_log() const {
            return transaction_log_;
        }

        bool is_empty() const {
            return (transaction_tail_ - writer_.tell()) == storage_.size();
        }

        void begin_transaction() {
            transaction_head_ = writer_.tell();
            transaction_tail_ = transaction_log_.select(-1).tail + storage_.size();

            writer_.reset(transaction_head_, transaction_tail_);
        }

        SequenceRange commit_transaction() {
            writer_.flush();
            transaction_log_.insert(writer_.tell());
            return transaction_log_.select(0);
        }

        // Returns a reference to the batch containing this operation sequence.
        //
        // NOTE: Reading an operation batch that hasn't been published in a transaction is undefined behavior.
        //
        const OperationBatch& read_batch(Sequence sequence) const {
            return storage_[sequence >> OperationBatch::SHIFT];
        }

        // Returns a reference to the operation corresponding to this sequence.
        //
        // NOTE: Reading an operation that hasn't been published in a transaction is undefined behavior.
        //
        const Operation& read(Sequence sequence) const {
            return read_batch(sequence).operations[sequence & OperationBatch::MASK];
        }

        // Adds an operation to the current, uncommitted transaction.
        // This can fail and return false if the ledger is full.
        bool write(Operation operation) {
            return writer_.write(operation);
        }

        // Return the number of entries that can still be written to the current transaction.
        size_t writable_transaction_entries() const {
            Sequence ceiling = transaction_log_.select(-1).tail + storage_.size();
            return ceiling - writer_.tell();
        }

    private:
        static constexpr size_t TRANSACTION_LOG_HISTORY = 4;

        using Storage = Ring<OperationBatch>;
        using Writer = OperationWriter<Storage>;

        Storage               storage_;
        SequenceRangeHistory  transaction_log_;
        Sequence              transaction_head_;
        Sequence              transaction_tail_;
        Writer                writer_;
    };

}
