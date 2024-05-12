#include <iostream>
#include <cstdlib>
#include <cassert>
#include "mantle/mantle.h"
#include "mantle/debug.h"
#include "fuzz.h"

std::string_view to_string(ActionType type) {
    using namespace std::literals;

    switch (type) {
#define X(ACTION_TYPE)                \
        case ActionType::ACTION_TYPE: \
            return #ACTION_TYPE ##sv; \

        ACTION_TYPES(X)
#undef X
    }

    abort(); // Unreachable.
}

Settings::Settings()
    : cycle_count(100)
    , worker_thread_count(1)
    , worker_object_count(1)
    , working_set_size(10)
{
    for (size_t& ratio: action_type_ratios) {
        ratio = 1;
    }
}

bool TestObject::is_alive() const {
    return (birth_count + 1) == death_count;
}

void TestObject::record_action(const Action& action) {
    std::scoped_lock<std::mutex> lock(action_log_mutex);

    action_log.push_back(action);
}

void TestObjectAllocator::Metrics::reset() {
    allocation_failure_count = 0;
    allocation_success_count = 0;
    deallocation_count       = 0;
}

TestObjectAllocator::TestObjectAllocator(WorkerThread& worker, size_t capacity)
    : worker_(worker)
{
    metrics_.reset();

    pool_.reserve(capacity);
    storage_.reserve(capacity);
    for (size_t i = 0; i < capacity; ++i) {
        pool_.push_back(
            storage_.emplace_back(std::make_unique<TestObject>()).get()
        );
    }
}

auto TestObjectAllocator::metrics() -> Metrics& {
    return metrics_;
}

Handle<TestObject> TestObjectAllocator::allocate_object() {
    if (pool_.empty()) {
        metrics_.allocation_failure_count += 1;
        return Handle<TestObject>();
    }

    TestObject* object = pool_.back();
    pool_.pop_back();

    metrics_.allocation_success_count += 1;
    object->birth_count += 1;

    return make_handle(*object);
}

void TestObjectAllocator::finalize(Object& object) noexcept {
    TestObject* test_object = &static_cast<TestObject&>(object);

    metrics_.deallocation_count += 1;
    test_object->death_count += 1;
    if (test_object->birth_count != test_object->death_count) {
        std::scoped_lock<std::mutex> lock(test_object->action_log_mutex);
        asm("int $3");
    }

    {
        Region& region = get_region();

        Action action = {
            .type             = ActionType::REAP,
            .object           = test_object,
            .source_region_id = region.id(),
            .target_region_id = region.id(),
            .state            = region.state(),
            .phase            = region.phase(),
            .cycle            = region.cycle(),
        };

        worker_.record_action(action);
    }

    test_object->old_action_log = std::move(test_object->action_log);
    test_object->action_log.clear();

    pool_.push_back(test_object);
}

ActionGenerator::ActionGenerator(const Settings& settings) {
    for (size_t action_type_index = 0; action_type_index < ACTION_TYPE_COUNT; ++action_type_index) {
        ActionType action_type = static_cast<ActionType>(action_type_index);

        size_t action_type_ratio = settings.action_type_ratios[action_type_index];
        for (size_t i = 0; i < action_type_ratio; ++i) {
            action_type_table_.push_back(action_type);
        }
    }

    std::random_device random_device;
    random_generator_ = std::mt19937(random_device());

    choice_distribution_      = std::uniform_int_distribution<size_t>(0, 1);
    target_distribution_      = std::uniform_int_distribution<size_t>(0, settings.worker_thread_count - 1);
    action_type_distribution_ = std::uniform_int_distribution<size_t>(0, action_type_table_.size() - 1);
}

bool ActionGenerator::random_choice() {
    return choice_distribution_(random_generator_) > 0;
}

ActionType ActionGenerator::random_action_type() {
    return action_type_table_.at(action_type_distribution_(random_generator_));
}

RegionId ActionGenerator::random_target() {
    return static_cast<RegionId>(target_distribution_(random_generator_));
}

void WorkerThread::Metrics::reset() {
    object_allocator.reset();

    for (size_t& counter: action_counts) {
        counter = 0;
    }
}

WorkerThread::WorkerThread(Driver& driver)
    : driver_(driver)
    , object_allocator_(*this, driver.settings().worker_object_count)
    , action_generator_(driver.settings())
    , metrics_(Metrics {
        .object_allocator = object_allocator_.metrics(),
        .action_counts    = {},
    })
{
    metrics_.reset();

    thread_ = std::thread([this]() {
        driver_.worker_thread_starting(*this);
        {
            Region region(driver_.domain(), object_allocator_);

            while (region.cycle() < driver_.settings().cycle_count) {
                step(region);
            }
        }
        driver_.worker_thread_stopping(*this);
    });
}

WorkerThread::~WorkerThread() {
    thread_.join();
}

auto WorkerThread::metrics() -> Metrics& {
    return metrics_;
}

void WorkerThread::step(Region& region) {
    Action action = {
        .type             = action_generator_.random_action_type(),
        .object           = nullptr,
        .source_region_id = region.id(),
        .target_region_id = region.id(),
        .state            = region.state(),
        .phase            = region.phase(),
        .cycle            = region.cycle(),
    };

    switch (action.type) {
        case ActionType::STEP: { // Step the `Region`.
            bool non_blocking = true;
            region.step(non_blocking);
            break;
        }
        case ActionType::MAKE: { // Allocate an object and add it to the working set.
            if (Handle<TestObject> object = object_allocator_.allocate_object()) {
                action.object = object.get();
                working_set_.push_back(std::move(object));
            }
            break;
        }
        case ActionType::REAP: { // Finalize the object. This happens on a different code-path.
            return;
        }
        case ActionType::DROP: { // Remove an object handle from the working set.
            if (!working_set_.empty()) {
                Handle<TestObject> object = std::move(working_set_.back());
                working_set_.pop_back();
                action.object = object.get();
            }
            break;
        }
        case ActionType::COPY: { // Add another handle for the object to the working set.
            if (!working_set_.empty() && (working_set_.size() < driver_.settings().working_set_size)) {
                Handle<TestObject> object = working_set_.back();
                action.object = object.get();
                working_set_.push_back(std::move(object));
            }
            break;
        }
        case ActionType::POKE: { // Generate local increment/decrement activity.
            if (!working_set_.empty()) {
                Handle<TestObject> object = working_set_.back();
                action.object = object.get();
                object.reset();
            }
            break;
        }
        case ActionType::SEND: { // Send an object (put one from our working set in a region's inbox).
            if (!working_set_.empty()) {
                Handle<TestObject> object = working_set_.back();
                action.object = object.get();
                action.target_region_id = action_generator_.random_target();

                // NOTE: Sending to ourself is fine.
                WorkerThread& other_worker_thread = driver_.worker_thread(action.target_region_id);
                other_worker_thread.deliver(Packet {
                    .source_region_id = action.source_region_id,
                    .object           = std::move(object),
                });
            }
            break;
        }
        case ActionType::RECV: { // Receive an object (check our inbox).
            Packet packet;
            if (receive(packet)) {
                action.object = packet.object.get();
                action.source_region_id = packet.source_region_id;

                working_set_.push_back(std::move(packet.object));
            }
            break;
        }
    }

    record_action(action);

    if (action.object) {
        action.object->record_action(action);
    }
}

void WorkerThread::deliver(Packet packet) {
    std::scoped_lock<std::mutex> lock(inbox_mutex_);

    inbox_.push_back(std::move(packet));
}

bool WorkerThread::receive(Packet& packet) {
    std::scoped_lock<std::mutex> lock(inbox_mutex_);

    if (inbox_.empty()) {
        return false;
    }

    packet = std::move(inbox_.front());
    inbox_.pop_front();
    return true;
}

void WorkerThread::record_action(const Action& action) {
    metrics_.action_counts[static_cast<size_t>(action.type)] += 1;
}

Driver::Driver(const Settings& settings)
    : settings_(settings)
    , starting_latch_(settings.worker_thread_count + 1)
    , stopping_latch_(settings.worker_thread_count + 1)
{
    for (size_t i = 0; i < settings.worker_thread_count; ++i) {
        worker_threads_.push_back(
            std::make_unique<WorkerThread>(*this)
        );
    }
}

const Settings& Driver::settings() const {
    return settings_;
}

Domain& Driver::domain() {
    return domain_;
}

WorkerThread& Driver::worker_thread(RegionId region_id) {
    return *worker_threads_.at(region_id);
}

void Driver::run() {
    // We need to participate in the starting latch to prevent
    // worker threads from accessing the worker thread vector
    // before it has finished being initialized. We also need
    // the release barrier in case `arrive_and_wait` is using
    // an acquire+release barrier instead of being sequentially
    // consistent (with the `clone` in `WorkerThread`).
    //
    // The stopping latch would be fine with just a `wait` call,
    // but the count would be one less than the starting latch.
    // And that would be weird.
    //
    starting_latch_.arrive_and_wait();
    stopping_latch_.arrive_and_wait();
}

void Driver::worker_thread_starting(WorkerThread&) {
    starting_latch_.arrive_and_wait();
}

void Driver::worker_thread_stopping(WorkerThread&) {
    stopping_latch_.arrive_and_wait();
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    size_t rounds = 1;
    for (size_t i = 0; i < rounds; ++i) {
        Settings settings;
        settings.cycle_count = 10000;
        settings.worker_thread_count = 6;
        settings.worker_object_count = 100;
        settings.action_type_ratios[static_cast<size_t>(ActionType::STEP)] = 1;
        settings.action_type_ratios[static_cast<size_t>(ActionType::MAKE)] = 2;
        settings.action_type_ratios[static_cast<size_t>(ActionType::POKE)] = 1;
        settings.action_type_ratios[static_cast<size_t>(ActionType::DROP)] = 3;
        settings.action_type_ratios[static_cast<size_t>(ActionType::RECV)] = 2;
        settings.action_type_ratios[static_cast<size_t>(ActionType::SEND)] = 1;

        Driver driver(settings);
        driver.run();

        if ((i % 100) == 0) {
            std::cout << fmt::format("{} / {}", i, rounds) << std::endl;
        }

        for (RegionId region_id = 0; region_id < settings.worker_thread_count; ++region_id) {
            const WorkerThread::Metrics& metrics = driver.worker_thread(region_id).metrics();

            std::cout << fmt::format("[worker_thread:{}] metrics", region_id) << std::endl;
            std::cout << fmt::format("\t[actions]") << std::endl;
            for (size_t action_type_index = 0; action_type_index < ACTION_TYPE_COUNT; ++action_type_index) {
                std::cout << fmt::format("\t.{} = {}", to_string(static_cast<ActionType>(action_type_index)), metrics.action_counts[action_type_index]) << std::endl;
            }
            std::cout << fmt::format("\t[allocations]") << std::endl;
            std::cout << fmt::format("\t  .allocation_failure_count = {}", metrics.object_allocator.allocation_failure_count) << std::endl;
            std::cout << fmt::format("\t  .allocation_success_count = {}", metrics.object_allocator.allocation_success_count) << std::endl;
            std::cout << fmt::format("\t  .deallocation_count       = {}", metrics.object_allocator.deallocation_count) << std::endl;
            std::cout << std::endl;
        }
    }

    return 0;
}
