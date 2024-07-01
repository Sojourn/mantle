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
        Ledger ledger(write_barrier_manager);

        Object object;

        WriteBarrier& barrier = ledger.decrement_barrier();
        for (size_t i = 0; i < 1024 * 1024; ++i) {
            decrement_ref_cnt(object);
        }
        ledger.step();
        ledger.step();
        for (size_t i = 0; i < 1024 * 1024; ++i) {
            increment_ref_cnt(object);
        }

        while (!barrier.is_empty()) {
            const WriteBarrierSegment& segment = *barrier.pop_back();
            std::cout << "inc_cnt: " << segment.increment_count << std::endl;
            std::cout << "dec_cnt: " << segment.decrement_count << std::endl;
            std::cout << std::endl;
        }

        done = true;
    });

    while (!done) {
        write_barrier_manager.poll();
    }

    thread.join();

    return EXIT_SUCCESS;
}
