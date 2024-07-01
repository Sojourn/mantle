#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include "mantle/mantle.h"
#include "mantle/ledger.h"
#include <thread>

using namespace mantle;

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    WriteBarrierManager write_barrier_manager;
    std::atomic_bool done = false;

    std::thread thread([&]() {
        for (size_t k = 0; k < 2001; k += 1) {
            for (size_t j = 0; j < 2001; j += 7) {
                Ledger ledger(write_barrier_manager);
                Object object;

                WriteBarrier& barrier = ledger.decrement_barrier();
                for (size_t i = 0; i < k; ++i) {
                    decrement_ref_cnt(object);
                }
                ledger.step();
                ledger.step();

                for (size_t i = 0; i < j; ++i) {
                    increment_ref_cnt(object);
                }
                ledger.step();

                assert(barrier.increment_count() == j);
                assert(barrier.decrement_count() == k);
            }
        }

        done = true;
    });

    while (!done) {
        write_barrier_manager.poll();
    }

    thread.join();

    return EXIT_SUCCESS;
}
