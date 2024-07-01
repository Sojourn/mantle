#include "catch.hpp"
#include "mantle/object.h"
#include "mantle/ledger.h"

using namespace mantle;

struct Counts {
    size_t increment_count = 0;
    size_t decrement_count = 0;
};

Counts counts(WriteBarrier& barrier) {
    Counts counts;

    WriteBarrierSegment* segment = barrier.back();
    while (segment) {
        counts.increment_count += segment->increment_count;
        counts.decrement_count += segment->decrement_count;
        segment = segment->prev;
    }

    return counts;
}

TEST_CASE("Ledger") {
    Object object;

    WriteBarrierManager write_barrier_manager;
    std::atomic_bool done = false;

    std::thread thread([&]() {
        Ledger ledger(write_barrier_manager);

        increment_ref_cnt(object);
        decrement_ref_cnt(object);

        WriteBarrier& inc_barrier = ledger.increment_barrier();
        WriteBarrier& dec_barrier = ledger.decrement_barrier();

        ledger.step();

        CHECK(counts(inc_barrier).increment_count == 1);
        CHECK(counts(dec_barrier).decrement_count == 1);

        done = true;
    });

    while (!done) {
        write_barrier_manager.poll();
    }

    thread.join();
}
