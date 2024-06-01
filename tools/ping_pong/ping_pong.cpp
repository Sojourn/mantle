#include "mantle/mantle.h"
#include <mutex>
#include <future>
#include <atomic>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <cassert>

using namespace mantle;

class WorkerThread final  : public ObjectFinalizer {
public:
    WorkerThread(Domain& domain, std::span<size_t> thread_affinity)
        : domain_(domain)
        , stopping_(false)
    {
        std::promise<void> start_promise;
        std::future<void> start_future = start_promise.get_future();

        thread_ = std::thread([thread_affinity, start_promise=std::move(start_promise), this]() mutable {
            try {
                set_cpu_affinity(thread_affinity);
                start_promise.set_value();
            }
            catch (const std::exception&) {
                start_promise.set_exception(std::current_exception());
                return;
            }

            Region region(domain_, *this);
            run(region);
            region.stop();
        });

        start_future.get();
    }

private:
    void run(Region& region) {
        while (!stopping_) {
            const bool non_blocking = true;
            region.step(non_blocking);
        }
    }

    void finalize(const ObjectGroup, const std::span<Object*> objects) noexcept override {
        for (const Object* object: objects) {
            delete object;
        }
    }

private:
    Domain&          domain_;
    std::thread      thread_;
    std::atomic_bool stopping_;
};

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    return EXIT_SUCCESS;
}
