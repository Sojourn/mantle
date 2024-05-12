# MANTLE
This library implements a reference counted smart pointers. It was designed for request-based workloads that alternate between idle and busy states.

## Safety Disclaimer
The core algorithm hasn't been checked in `TLA+`, yet.

## Example
```
#include <new>
#include <mantle/mantle.h>

int main() {
    mantle::Domain domain;

    std::thread([&]() {
        // A trivial object finalizer that simply calls `operator delete`.
        // In practice this should be forwarding the call to an appropriate
        // thread-local allocator.
        struct : mantle::ObjectFinalizer {
            void finalize(mantle::Object& object) override final noexcept {
                delete &object;
            }
        } finalizer;

        // Register this thread with the domain so that handles may be used.
        mantle::Region region(domain, finalizer);

        // event_loop.add_reader(region.file_descriptor(), [&]() {
        //     region.step();
        // });

        {
            // Bind an object to a handle. Note that the library is not doing the allocation.
            mantle::Handle<Object> h1 = make_handle(*(new Object));
        }

        // This will block until all other threads are also ready to stop, and all
        // objects have either been collected or leaked. It is called implicitly
        // when the region goes out of scope.
        region.stop();

    }).join();
}

```

## Design Goals
### Thread Safety
Ensure that objects are not prematurely destroyed while they are still in use by multiple threads. This is critical to prevent invalid memory access and ensure data integrity.

### Weighted References
Support handles that can maintain multiple references to an object. These handles should be capable of donating a portion of their references (e.g., half) to facilitate operations like copying without requiring synchronization. This feature helps in amortizing the cost associated with frequent updates to the reference count.

### Deferred Finalization
Implement a mechanism where objects are not immediately finalized when the last reference is dropped. This approach is particularly useful in environments like Python, where immediate finalization of large object trees can lead to excessive stack usage. Deferred finalization helps in managing resource cleanup more efficiently and safely.

### Lazy Reference Count Updates
Delay the execution of reference count updates until a more opportune time, ideally when the application is less busy or idle. This strategy helps in reducing the impact of random memory accesses on the system’s performance by minimizing cache misses and smoothing out processing time variability.

### Destructive Interference
Optimize the system to recognize and negate offsetting reference count updates (e.g., (+1, -1)) whenever possible. By not applying these neutral updates, the system can reduce unnecessary computational overhead and improve overall efficiency.

### Object Affinity
Prefer that the thread which allocates an object is also responsible for its finalization. This alignment allows thread-local allocators to operate more efficiently by minimizing the need for locking mechanisms, even when objects they allocate are accessed by multiple threads.

### Handle Size
Ensure that the size of the handle is minimal, ideally not exceeding the size of a pointer (sizeof(void*)). This requirement is important for maintaining memory efficiency, especially in systems where numerous handles are in use.

### Intrusive Object Meta-data
The meta-data block, which includes the reference count among other data, should be part of the object itself (intrusive). This design avoids the need for separate allocations for meta-data, thereby simplifying memory management and reducing allocation overhead.

## Algorithm Overview
The algorithm manages reference count updates, which can be represented as pairs (e.g., (+1, -1) for standard reference counting and (+128, {-64, -32, -32}) for weighted reference counting). For simplicity and clarity in this explanation, we'll focus on the standard model of (+1, -1) pairs. The primary challenge addressed by this algorithm is ensuring that increments (+1) are applied before their corresponding decrements (-1) across multiple threads.

### Epoch-Based Synchronization
To handle the synchronization of reference count updates safely across threads, the algorithm employs an epoch-based approach. Each epoch represents a discrete time unit during which specific actions related to reference count updates are performed. The sequence of actions across epochs ensures that all increments are applied before any corresponding decrements, even in a multi-threaded scenario.

### Detailed Steps
 * `T=0`: Reference count updates are collected in a thread-local batch. This batch includes both increments and decrements, but they are not yet applied.
 * `T=1`: Increments from the current thread's batch are applied immediately. Increments from other threads may also be applied during this epoch, depending on the timing of thread execution (potential race conditions).
 * `T=2`: Any remaining increments from other threads that were not applied in `T=1` due to race conditions are now applied. This ensures that all increments are applied before moving to the next step.
 * `T=3`: Decrement operations from the current thread's batch are applied. By this epoch, all corresponding increments (from any thread) should have been applied, ensuring the safe decrement of reference counts. Similar to increments, decrements from other threads may also be applied during this epoch if they are ready and no races affect their order.
 * `T>3`: Any remaining decrements from other threads that were not applied in `T=3` due to race conditions are now applied.

## TODO
 * Check the model in `TLA+`. This is definitely the most important pending task, since everything else is pointless if the algorithm isn't sound.
 * Benchmarks versus `std::shared_ptr<T>`. I'm interested in what the latency, throughput, and contention differences look like.
 * Profiling and optimization. So far I've been mostly concerned with getting all of this working.
 * Add examples.
 * `Region::step` should incrementally finalize objects. I'd prefer better tails and marginally worse averages (latency, cache impact). Also, we have no idea how long finalization takes. Maybe break out after some number of microseconds to stay responsive?
