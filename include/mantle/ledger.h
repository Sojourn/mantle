#pragma once

#include <span>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <cstddef>
#include <cstdint>

#include "mantle/config.h"
#include "mantle/types.h"
#include "mantle/util.h"
#include "mantle/page_fault_handler.h"

#define WRITE_BARRIER_PHASES(X) \
    X(STORE_DECREMENTS)         \
    X(DELAY)                    \
    X(STORE_INCREMENTS)         \
    X(APPLY)                    \

namespace mantle {

    class Ledger;
    class WriteBarrier;

    enum class WriteBarrierPhase {
#define X(WRITE_BARRIER_PHASE) WRITE_BARRIER_PHASE,
        WRITE_BARRIER_PHASES(X)
#undef X
    };

    constexpr size_t WRITE_BARRIER_PHASE_COUNT = 0
#define X(WRITE_BARRIER_PHASE) + 1
        WRITE_BARRIER_PHASES(X)
#undef X
    ;

    // This is a simple RAII wrapper around a private anonymous memory mapping.
    class PrivateMemoryMapping {
    public:
        explicit PrivateMemoryMapping(size_t size, bool populate = true);
        ~PrivateMemoryMapping();

        PrivateMemoryMapping(PrivateMemoryMapping&&) = delete;
        PrivateMemoryMapping(const PrivateMemoryMapping&) = delete;
        PrivateMemoryMapping& operator=(PrivateMemoryMapping&&) = delete;
        PrivateMemoryMapping& operator=(const PrivateMemoryMapping&) = delete;

        [[nodiscard]]
        std::span<std::byte> memory();

        [[nodiscard]]
        std::span<const std::byte> memory() const;

    private:
        std::span<std::byte> memory_;
    };

    struct WriteBarrierSegment {
        WriteBarrierSegment* prev;
        WriteBarrier*        barrier;
        PrivateMemoryMapping mapping;
        bool                 primed; // Write protection status.
        size_t               increment_count;
        size_t               decrement_count;

        WriteBarrierSegment();

        WriteBarrierSegment(WriteBarrierSegment&&) = delete;
        WriteBarrierSegment(const WriteBarrierSegment&) = delete;
        WriteBarrierSegment& operator=(WriteBarrierSegment&&) = delete;
        WriteBarrierSegment& operator=(const WriteBarrierSegment&) = delete;

        Object** cursor();
        std::span<Object*> records();
        std::span<Object*> increment_records();
        std::span<Object*> decrement_records();
        std::span<std::byte> guard_page();
    };

    // Rename to WriteBarrierStack?
    class WriteBarrier {
    public:
        using Phase = WriteBarrierPhase;

        explicit WriteBarrier(Ledger& ledger, size_t phase_shift);

        WriteBarrier(WriteBarrier&&) = delete;
        WriteBarrier(const WriteBarrier&) = delete;
        WriteBarrier& operator=(WriteBarrier&&) = delete;
        WriteBarrier& operator=(const WriteBarrier&) = delete;

        Ledger& ledger();

        [[nodiscard]]
        Phase phase() const;

        [[nodiscard]]
        bool is_empty() const;

        // TODO: Give these `_segment` suffixes.
        WriteBarrierSegment* back();
        void push_back(WriteBarrierSegment& segment);
        WriteBarrierSegment* pop_back();

        void commit();

        // NOTE: This is O(#segments).
        [[nodiscard]]
        size_t increment_count() const;

        // NOTE: This is O(#segments).
        [[nodiscard]]
        size_t decrement_count() const;

    private:
        Ledger&              ledger_;
        size_t               phase_shift_;
        WriteBarrierSegment* stack_; // Top of the stack.
    };

    class WriteBarrierManager {
    public:
        WriteBarrierManager();

        [[nodiscard]]
        int file_descriptor();
        void poll(bool non_blocking);

        void attach(WriteBarrier& barrier);
        void detach(WriteBarrier& barrier);

        WriteBarrierSegment& allocate_segment();
        void deallocate_segment(WriteBarrierSegment& segment);

    private:
        void prime_guard_page(WriteBarrierSegment& segment);

    private:
        PageFaultHandler                                  page_fault_handler_;

        std::mutex                                        segment_pool_mutex_;
        std::vector<WriteBarrierSegment*>                 segment_pool_;
        std::vector<std::unique_ptr<WriteBarrierSegment>> segment_pool_storage_;
    };

    class Ledger {
    public:
        // Conceptually a `std::atomic<std::vector<Object*>::iterator>`.
        using Cursor = std::atomic<Object**>;

        static Cursor& local_increment_cursor() {
            thread_local Cursor cursor = nullptr;
            return cursor;
        }

        static Cursor& local_decrement_cursor() {
            thread_local Cursor cursor = nullptr;
            return cursor;
        }

        explicit Ledger(WriteBarrierManager& write_barrier_manager);
        ~Ledger();

        Ledger(Ledger&&) = delete;
        Ledger(const Ledger&) = delete;
        Ledger& operator=(Ledger&&) = delete;
        Ledger& operator=(const Ledger&) = delete;

        [[nodiscard]]
        Sequence sequence() const;

        [[nodiscard]]
        bool is_empty() const;

        Cursor& increment_cursor();
        Cursor& decrement_cursor();

        // Find the barrier in the corresponding phase.
        WriteBarrier& barrier(WriteBarrierPhase phase);
        WriteBarrier& increment_barrier();
        WriteBarrier& decrement_barrier();

        // Advances barrier phases and returns a barrier that can be applied.
        WriteBarrier& commit();

    private:
        AtomicSequence       sequence_;
        Cursor&              increment_cursor_;
        Cursor&              decrement_cursor_;
        WriteBarrier         write_barriers_[WRITE_BARRIER_PHASE_COUNT];
        WriteBarrierManager& write_barrier_manager_;
    };

    MANTLE_HOT
    void increment_ref_cnt(Object& object) {
        std::atomic<Object**>& cursor = Ledger::local_increment_cursor();
        Object** record = cursor.load(std::memory_order_acquire); // Doesn't need to be a fetch-add.
        cursor.store(record + 1, std::memory_order_release);
        *record = &object;
    }

    MANTLE_HOT
    void decrement_ref_cnt(Object& object) {
        std::atomic<Object**>& cursor = Ledger::local_decrement_cursor();
        Object** record = cursor.load(std::memory_order_acquire); // Doesn't need to be a fetch-add.
        cursor.store(record + 1, std::memory_order_release);
        *record = &object;
    }

    using ObjectLedger = Ledger;

}
