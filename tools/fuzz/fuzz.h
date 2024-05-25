#pragma once

#include <random>
#include <memory>
#include <vector>
#include <queue>
#include <latch>
#include <thread>
#include <cstdint>
#include <cstddef>
#include "mantle/mantle.h"

#define ACTION_TYPES(X) \
    X(STEP)             \
    X(MAKE)             \
    X(REAP)             \
    X(DROP)             \
    X(COPY)             \
    X(POKE)             \
    X(SEND)             \
    X(RECV)             \

using namespace mantle;

class Driver;
class WorkerThread;

struct TestObject;

enum class ActionType {
#define X(ACTION_TYPE) \
    ACTION_TYPE,       \

    ACTION_TYPES(X)
#undef X
};

constexpr size_t ACTION_TYPE_COUNT = 0
#define X(ACTION_TYPE) \
    + 1                \

    ACTION_TYPES(X)
#undef X
;

std::string_view to_string(ActionType type);

struct Action {
    ActionType    type;
    TestObject*   object;
    RegionId      source_region_id;
    RegionId      target_region_id;
    Region::State state;
    Region::Phase phase;
    Region::Cycle cycle;
};

struct Settings {
    std::array<size_t, ACTION_TYPE_COUNT> action_type_ratios;

    size_t cycle_count;
    size_t worker_thread_count;
    size_t worker_object_count;
    size_t working_set_size;

    Settings();
};

struct TestObject : Object {
    size_t birth_count = 0;
    size_t death_count = 0;

    std::vector<Action> action_log;
    std::vector<Action> old_action_log;
    std::mutex          action_log_mutex;

    bool is_alive() const;

    void record_action(const Action& action);
};

class TestObjectAllocator final : public ObjectFinalizer {
public:
    struct Metrics {
        size_t allocation_failure_count;
        size_t allocation_success_count;
        size_t deallocation_count;

        void reset();
    };

    TestObjectAllocator(WorkerThread& worker, size_t capacity);

    Metrics& metrics();

    Handle<TestObject> allocate_object();

    void finalize(ObjectGroup, std::span<Object*> objects) noexcept override;

private:
    WorkerThread&                            worker_;
    std::vector<TestObject*>                 pool_;
    std::vector<std::unique_ptr<TestObject>> storage_;
    Metrics                                  metrics_;
};

class ActionGenerator {
public:
    explicit ActionGenerator(const Settings& settings);

    bool random_choice();
    RegionId random_target();
    ActionType random_action_type();

private:
    std::mt19937                          random_generator_;
    std::uniform_int_distribution<size_t> choice_distribution_;
    std::uniform_int_distribution<size_t> target_distribution_;
    std::uniform_int_distribution<size_t> action_type_distribution_;
    std::vector<ActionType>               action_type_table_;
};

class WorkerThread {
public:
    struct Metrics {
        TestObjectAllocator::Metrics& object_allocator;

        std::array<size_t, ACTION_TYPE_COUNT> action_counts;

        void reset();
    };

    explicit WorkerThread(Driver& driver);
    ~WorkerThread();

    Metrics& metrics();

private:
    friend class TestObjectAllocator;

    struct Packet {
        RegionId           source_region_id;
        Handle<TestObject> object;
    };

    void step(Region& region);

    void deliver(Packet packet);
    bool receive(Packet& packet);

    void record_action(const Action& action);

private:
    using Inbox = std::deque<Packet>;
    using WorkingSet = std::vector<Handle<TestObject>>;

    Driver&             driver_;
    std::thread         thread_;

    TestObjectAllocator object_allocator_;
    ActionGenerator     action_generator_;

    std::mutex          inbox_mutex_;
    Inbox               inbox_;
    WorkingSet          working_set_;

    Metrics             metrics_;
};

class Driver {
public:
    explicit Driver(const Settings& settings);

    const Settings& settings() const;

    Domain& domain();
    WorkerThread& worker_thread(RegionId region_id);

    void run();

private:
    friend class WorkerThread;

    void worker_thread_starting(WorkerThread& worker_thread);
    void worker_thread_stopping(WorkerThread& worker_thread);

private:
    Settings                                   settings_;
    std::latch                                 starting_latch_;
    std::latch                                 stopping_latch_;

    Domain                                     domain_;
    std::vector<std::unique_ptr<WorkerThread>> worker_threads_;
};
