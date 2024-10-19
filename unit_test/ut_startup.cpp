#include "catch.hpp"
#include "mantle/mantle.h"
#include "ut_common.h"

#include <thread>
#include <barrier>

using namespace mantle;

TEST_CASE("Startup") {
    SECTION("Many regions at once") {
        Domain domain;

        // Enough that threads will be concurrently starting and stopping.
        std::vector<std::jthread> threads;
        for (int i = 0; i < 10000; ++i) {
            threads.emplace_back([&domain] {
                CommonObjectFinalizer finalizer;
                Region region(domain, finalizer);
                Ref<Object> ref = make_object();
            });
        }
    }
}
