#include "benchmark.h"
#include <iostream>
#include <vector>
#include <latch>
#include <thread>

using namespace mantle;

struct TrivialFinalizer : ObjectFinalizer {
    void finalize(Object&) noexcept override final {}
};

// Decrement the latch and step the region until it is safe to proceed.
inline void synchronize(Region& region, std::latch& latch) {
    latch.count_down();
    while (!latch.try_wait()) {
        bool non_blocking = true;
        region.step(non_blocking);
    }
}

void contend_mantle_handle(size_t thread_count, size_t iterations, size_t object_count) {
    std::latch running_latch(thread_count + 1);
    std::latch stopped_latch(thread_count + 1);

    Config config;
    config.operation_grouper_enabled = false;
    config.operation_shuffler_enabled = false;

    Domain domain(config);
    TrivialFinalizer finalizer;
    Region root_region(domain, finalizer);

    std::vector<Object> objects(object_count);
    std::vector<Handle<Object>> root_handles;
    root_handles.reserve(object_count);
    for (size_t i = 0; i < object_count; ++i) {
        root_handles.push_back(make_handle(objects[i]));
    }

    std::vector<std::jthread> threads;
    threads.reserve(thread_count);
    for (size_t i = 0; i < thread_count; ++i) {
        std::vector<Handle<Object>> handles = root_handles;

        threads.push_back(std::jthread([&, handles=std::move(handles)]() mutable {
            // TODO: Set the thread's affinity.

            TrivialFinalizer finalizer;
            Region region(domain, finalizer);

            std::cout << "Worker starting\n";
            synchronize(region, running_latch);

            for (size_t i = 0; i < iterations; ++i) {
                for (Handle<Object>& handle: handles) {
                    Handle<Object> handle_copy = handle;
                    (void)handle_copy;
                }
            }
            handles.clear();

            region.stop();
            std::cout << "Worker stopping\n";
            synchronize(region, stopped_latch);
        }));
    }
    root_handles.clear();

    std::cout << "Main thread starting\n";
    synchronize(root_region, running_latch);

    root_region.stop();
    std::cout << "Main thread stopping\n";
    synchronize(root_region, stopped_latch);
}

int main() {
    size_t thread_count = 2;
    size_t iterations = 1000;
    size_t object_count = 1000;

    contend_mantle_handle(thread_count, iterations, object_count);

    return 0;
}
