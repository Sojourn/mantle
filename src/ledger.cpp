#include "mantle/ledger.h"
#include "mantle/page_fault_handler.h"
#include <sys/mman.h>
#include <cstring>
#include <cassert>

namespace mantle {

    PrivateMemoryMapping::PrivateMemoryMapping(const size_t size, const bool populate) {
        assert(size >= PAGE_SIZE);
        assert((size % PAGE_SIZE) == 0);

        void* address = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (address == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }

        memory_ = std::span(static_cast<std::byte*>(address), size);

        if (populate) {
            // Touch the first byte of each page to pre-fault the memory.
            for (size_t i = 0; i < memory_.size_bytes(); i += PAGE_SIZE) {
                const_cast<volatile std::byte&>(memory_[i]) = std::byte{0};
            }
        }
    }

    PrivateMemoryMapping::~PrivateMemoryMapping() {
        const int result = munmap(memory_.data(), memory_.size());
        assert(result >= 0);
    }

    std::span<std::byte> PrivateMemoryMapping::memory() {
        return memory_;
    }

    std::span<const std::byte> PrivateMemoryMapping::memory() const {
        return memory_;
    }

    WriteBarrierSegment::WriteBarrierSegment()
        : prev(nullptr)
        , barrier(nullptr)
        , primed(false)
        , increment_count(0)
        , decrement_count(0)
        , mapping(WRITE_BARRIER_CAPACITY * sizeof(Object*), true)
    {
    }

    Object** WriteBarrierSegment::cursor() {
        return &objects()[increment_count + decrement_count];
    }

    std::span<Object*> WriteBarrierSegment::objects() {
        return std::span{reinterpret_cast<Object**>(mapping.memory().data()), mapping.memory().size_bytes() / sizeof(Object*)};
    }

    std::span<std::byte> WriteBarrierSegment::guard_page() {
        return mapping.memory().last(PAGE_SIZE);
    }

    WriteBarrier::WriteBarrier(Ledger& ledger, const size_t phase_shift)
        : ledger_(ledger)
        , phase_shift_(phase_shift)
        , stack_(nullptr)
    {
        assert(phase_shift_ < WRITE_BARRIER_PHASE_COUNT);
    }

    WriteBarrier::~WriteBarrier() {
        assert(is_empty());
    }

    Ledger& WriteBarrier::ledger() {
        return ledger_;
    }

    auto WriteBarrier::phase() const -> Phase {
        return static_cast<Phase>((ledger_.sequence() + phase_shift_) % WRITE_BARRIER_PHASE_COUNT);
    }

    bool WriteBarrier::is_empty() const {
        return stack_ == nullptr;
    }

    WriteBarrierSegment* WriteBarrier::back() {
        return stack_;
    }

    void WriteBarrier::push_back(WriteBarrierSegment& segment) {
        assert(!segment.barrier);
        assert(!segment.prev);
        assert(segment.increment_count == 0);
        assert(segment.decrement_count == 0);
        assert(segment.primed);

        segment.barrier = this;
        segment.prev = stack_;

        switch (phase()) {
            case WriteBarrierPhase::STORE_INCREMENTS: {
                ledger_.increment_cursor().store(segment.cursor(), std::memory_order_release);
                break;
            }
            case WriteBarrierPhase::STORE_DECREMENTS: {
                ledger_.decrement_cursor().store(segment.cursor(), std::memory_order_release);
                break;
            }
            default: {
                break; // This segment is not active.
            }
        }

        stack_ = &segment;
    }

    WriteBarrierSegment* WriteBarrier::pop_back() {
        if (!stack_) {
            return nullptr;
        }

        switch (phase()) {
            case WriteBarrierPhase::STORE_INCREMENTS: {
                ledger_.increment_cursor().store(nullptr, std::memory_order_release);
                break;
            }
            case WriteBarrierPhase::STORE_DECREMENTS: {
                ledger_.decrement_cursor().store(nullptr, std::memory_order_release);
                break;
            }
            default: {
                break; // This segment is not active.
            }
        }

        return std::exchange(stack_, stack_->prev);
    }

    void WriteBarrier::commit(const bool pending_write) {
        assert(stack_);

        if (pending_write) {
            stack_->primed = false;
        }

        switch (phase()) {
            case Phase::STORE_INCREMENTS: {
                auto first = stack_->cursor();
                auto last = ledger_.increment_cursor().load(std::memory_order_acquire);
                stack_->increment_count = last - first;
                break;
            }
            case Phase::STORE_DECREMENTS: {
                auto first = stack_->cursor();
                auto last = ledger_.decrement_cursor().load(std::memory_order_acquire);
                stack_->decrement_count = last - first;
                break;
            }
            default: {
                abort();
            }
        }
    }

    WriteBarrierManager::WriteBarrierManager() {
        // TODO: Size the segment storage/pool based on the number of threads.
    }

    int WriteBarrierManager::file_descriptor() {
        return page_fault_handler_.file_descriptor();
    }

    void WriteBarrierManager::poll() {
        page_fault_handler_.poll([this](std::span<const std::byte> memory, PageFaultHandler::Mode mode) {
            if (mode == PageFaultHandler::Mode::WRITE_PROTECT) {
                WriteBarrierSegment* prev_segment;
                memcpy(&prev_segment, memory.data(), sizeof(prev_segment));

                WriteBarrier& barrier = *prev_segment->barrier;
                barrier.commit(true);

                WriteBarrierSegment& next_segment = allocate_segment();
                assert(next_segment.primed);
                barrier.push_back(next_segment);

                // Allow the pending write to proceed now that the next segment has been installed.
                page_fault_handler_.write_unprotect_memory(prev_segment->guard_page());
            }
            else {
                abort();
            }
        });
    }

    void WriteBarrierManager::attach(WriteBarrier& barrier) {
        WriteBarrierSegment& segment = allocate_segment();
        barrier.push_back(segment);
    }

    void WriteBarrierManager::detach(WriteBarrier& barrier) {
        while (WriteBarrierSegment* segment = barrier.pop_back()) {
            deallocate_segment(*segment);
        }
    }

    void WriteBarrierManager::prime_guard_page(WriteBarrierSegment& segment) {
        if (segment.primed) {
            return;
        }

        const WriteBarrierSegment* segment_address = &segment;
        std::span<std::byte> guard_page = segment.guard_page();
        memcpy(guard_page.data(), &segment_address, sizeof(segment_address));

        page_fault_handler_.write_protect_memory(guard_page);
        segment.primed = true;
    }

    WriteBarrierSegment& WriteBarrierManager::allocate_segment() {
        std::scoped_lock lock(segment_pool_mutex_);

        WriteBarrierSegment* segment = nullptr;

        if (UNLIKELY(segment_pool_.empty())) {
            segment = segment_pool_storage_.emplace_back(std::make_unique<WriteBarrierSegment>()).get();
            page_fault_handler_.register_memory(segment->guard_page(), {PageFaultHandler::Mode::WRITE_PROTECT});
        }
        else {
            segment = segment_pool_.back();
            segment_pool_.pop_back();
        }

        prime_guard_page(*segment);
        return *segment;
    }

    void WriteBarrierManager::deallocate_segment(WriteBarrierSegment& segment) {
        std::scoped_lock lock(segment_pool_mutex_);

        segment.barrier = nullptr;
        segment.prev = nullptr;
        segment.increment_count = 0;
        segment.decrement_count = 0;

        segment_pool_.push_back(&segment);
    }

    Ledger::Ledger(WriteBarrierManager& write_barrier_manager)
        : sequence_(0)
        , increment_cursor_(local_increment_cursor())
        , decrement_cursor_(local_decrement_cursor())
        , write_barriers_{
            WriteBarrier{*this, 0},
            WriteBarrier{*this, 1},
            WriteBarrier{*this, 2},
            WriteBarrier{*this, 3},
        }
        , write_barrier_manager_(write_barrier_manager)
    {
        for (auto&& barrier : write_barriers_) {
            write_barrier_manager_.attach(barrier);
        }
    }

    Ledger::~Ledger() {
        for (auto&& barrier : write_barriers_) {
            write_barrier_manager_.detach(barrier);
        }
    }

    Sequence Ledger::sequence() const {
        return sequence_.load(std::memory_order_acquire);
    }

    auto Ledger::increment_cursor() -> Cursor& {
        return increment_cursor_;
    }

    auto Ledger::decrement_cursor() -> Cursor& {
        return decrement_cursor_;
    }

    WriteBarrier& Ledger::barrier(const WriteBarrierPhase phase) {
        const Sequence sequence = sequence_.load(std::memory_order_acquire);

        WriteBarrier& barrier = write_barriers_[(static_cast<uint64_t>(phase) - sequence) % WRITE_BARRIER_PHASE_COUNT];
        assert(phase == barrier.phase());
        return barrier;
    }

    WriteBarrier& Ledger::increment_barrier() {
        return barrier(WriteBarrierPhase::STORE_INCREMENTS);
    }

    WriteBarrier& Ledger::decrement_barrier() {
        return barrier(WriteBarrierPhase::STORE_DECREMENTS);
    }

    void Ledger::step() {
        increment_barrier().commit(false);
        decrement_barrier().commit(false);

        sequence_.fetch_add(1, std::memory_order_acq_rel);

        increment_cursor_.store(increment_barrier().back()->cursor(), std::memory_order_release);
        decrement_cursor_.store(decrement_barrier().back()->cursor(), std::memory_order_release);
    }

}
