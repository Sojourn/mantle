#pragma once

#include <vector>
#include <cstring>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include <emmintrin.h>
#include "mantle/types.h"
#include "mantle/util.h"
#include "mantle/operation.h"

namespace mantle {

    // This class streams batches of operations to memory, bypassing the CPU cache hierarchy.
    template<typename Storage_>
    class OperationWriter {
    public:
        using Storage = Storage_;

        OperationWriter(Storage& storage, Sequence head = 0, Sequence tail = 0)
            : storage_(storage)
            , head_(0)
            , tail_(0)
        {
            memset(&batch_, 0, sizeof(batch_));

            reset(head, tail);
        }

        OperationWriter(OperationWriter&&) = delete;
        OperationWriter(const OperationWriter&) = delete;
        OperationWriter& operator=(OperationWriter&&) = delete;
        OperationWriter& operator=(const OperationWriter&) = delete;

        Sequence tell() const {
            return head_;
        }

        // TODO: Look at the code-gen for this. I think it might suck.
        //       We want a fairly short instruction sequence so it can inline.
        MANTLE_HOT bool write(Operation operation) {
            if (head_ == tail_) {
                return false;
            }

            size_t operation_index = head_ & OperationBatch::MASK;
            batch_.operations[operation_index] = operation;

            // Stream the batch to memory if we just completed it.
            if (operation_index == OperationBatch::MASK) {
                size_t batch_index = head_ >> OperationBatch::SHIFT;

                OperationBatch* target_batch = &storage_[batch_index];
                __m128i* target_pointer = (__m128i*)target_batch;

                const OperationBatch* source_batch = &batch_;
                const __m128i* source_pointer = (const __m128i*)source_batch;

                _mm_stream_si128(target_pointer+0, _mm_load_si128(source_pointer+0));
                _mm_stream_si128(target_pointer+1, _mm_load_si128(source_pointer+1));
                _mm_stream_si128(target_pointer+2, _mm_load_si128(source_pointer+2));
                _mm_stream_si128(target_pointer+3, _mm_load_si128(source_pointer+3));
            }

            head_ += 1;

            return true;
        }

        // Pad the current batch with null operations and write it out if it is partially full.
        //
        // !!! This must be called to make prior writes visible in other threads. !!!
        //
        void flush() {
            while (head_ & OperationBatch::MASK) {
                write(make_null_operation());
            }

            _mm_sfence();
        }

        void reset(Sequence head = 0, Sequence tail = 0) {
            assert(!(head & OperationBatch::MASK));
            assert(!(tail & OperationBatch::MASK));

            head_ = head;
            tail_ = tail;
        }

    private:
        Storage&       storage_;
        Sequence       head_;
        Sequence       tail_;
        OperationBatch batch_;
    };

    using OperationVector = std::vector<OperationBatch>;

    class OperationVectorWriter : private OperationWriter<OperationVector> {
    public:
        using Base = OperationWriter<OperationVector>;

        OperationVectorWriter(size_t capacity = 0)
            : Base(storage_)
        {
            storage_.reserve(capacity);
        }

        OperationRange data() {
            return {
                .head = storage_.data(),
                .tail = storage_.data() + storage_.size(),
            };
        }

        std::span<OperationBatch> span() {
            return {
                storage_.data(),
                storage_.size(),
            };
        }

        void write(Operation operation) {
            // Fast-path: the current batch has space for this operation.
            if (LIKELY(Base::write(operation))) {
                return;
            }

            // Append a new, empty batch.
            {
                Sequence new_head = Base::tell();
                Sequence new_tail = new_head + OperationBatch::SIZE;

                OperationBatch batch;
                memset(&batch, 0, sizeof(batch));
                storage_.push_back(batch);

                Base::reset(new_head, new_tail);
            }

            // This write will succeed with the additional capacity.
            bool written = Base::write(operation);
            (void)written;
            assert(written); // TODO: Make a `MANTLE_ASSERT` macro that does a void cast in release builds.
        }

        void flush() {
            Base::flush();
        }

        void clear() {
            storage_.clear();
            Base::reset(0, 0);
        }

        std::vector<OperationBatch> release() {
            return std::move(storage_);
        }

    private:
        std::vector<OperationBatch> storage_;
    };

}
