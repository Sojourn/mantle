# MANTLE
This library implements reference counted smart pointers in a way that attempts to mitigate or defer the typical costs associated with it. At a high level each thread in the application has vectors of deferred reference count updates which are periodically synchronized and applied by a worker thread.

* Contention on the reference counter is avoided by performing all updates on a single worker thread. This also allows the updates to use normal, unlocked arithmetic instructions instead of the more expensive atomic variants.
* Branching to check if an object is not done in the critical path since updates are deferred.
* Appending to the thread local update vector is branchless. Bounds checking is done using guard pages and extending vectors as needed when write protection faults are raised.
* Memory accesses due to reference counting are linear on application threads.

## Codegen
```
void copy(Ref<Object>& dst, const Ref<Object>& src) {
    dst = src; // Decrements dst and increments src, which may reference the same object.
}
```

```
copy(mantle::Ref<mantle::Object>&, mantle::Ref<mantle::Object> const&):
        mov     rdx, QWORD PTR [rdi]
        mov     rax, QWORD PTR fs:mantle::Ledger::local_decrement_cursor()::cursor@tpoff
        lea     rcx, [rax+8]
        mov     QWORD PTR fs:mantle::Ledger::local_decrement_cursor()::cursor@tpoff, rcx
        mov     QWORD PTR [rax], rdx
        mov     rdx, QWORD PTR [rsi]
        mov     QWORD PTR [rdi], rdx
        mov     rax, QWORD PTR fs:mantle::Ledger::local_increment_cursor()::cursor@tpoff
        lea     rcx, [rax+8]
        mov     QWORD PTR fs:mantle::Ledger::local_increment_cursor()::cursor@tpoff, rcx
        mov     QWORD PTR [rax], rdx
        ret
```

## TODO
* Updating reference counts can be parallelized. Worker Threads can do a radix sort on per-region submissions to make it embarrassingly parallel.
