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
        for (size_t k = 0; k < 2001; k += 13) {
            for (size_t j = 0; j < 2001; j += 69) {
                Ledger ledger(write_barrier_manager);
                Object object;

                WriteBarrier& barrier = ledger.decrement_barrier();
                for (size_t i = 0; i < k; ++i) {
                    decrement_ref_cnt(object);
                }
                ledger.commit();
                ledger.commit();

                for (size_t i = 0; i < j; ++i) {
                    increment_ref_cnt(object);
                }
                ledger.commit();

                assert(barrier.increment_count() == j);
                assert(barrier.decrement_count() == k);
            }
        }

        done = true;
    });

    while (!done) {
        constexpr bool non_blocking = true;
        write_barrier_manager.poll(non_blocking);
    }

    thread.join();

    return EXIT_SUCCESS;
}
