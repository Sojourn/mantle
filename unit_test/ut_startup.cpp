#include "catch.hpp"
#include "mantle/mantle.h"
#include "ut_common.h"

#include <thread>
#include <latch>

using namespace mantle;

TEST_CASE("Startup") {
    SECTION("Many regions at once") {
        Domain domain;

        // Enough that threads will be concurrently starting and stopping.
        std::vector<std::jthread> threads(100);
        std::latch latch(threads.size());
        for (std::jthread& thread : threads) {
            thread = std::jthread([&domain, &latch] {
                CommonObjectFinalizer finalizer;
                Region region(domain, finalizer);
                latch.count_down();

                Ref<Object> ref = make_object();
            });
        }

        latch.wait();
    }
}
