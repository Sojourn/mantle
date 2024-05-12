#pragma once

#include <optional>
#include <span>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include "mantle/config.h"
#include "mantle/operation.h"

namespace mantle {

    // A std::span<OperationBatch> is effectively a 2-d array of operations.
    // This class maps that onto a 1-d array which is more convenient for sorting.
    class OperationSlice {
    public:
        OperationSlice();
        explicit OperationSlice(std::span<OperationBatch> array);
        OperationSlice(std::span<OperationBatch> array, size_t first, size_t last);

        bool is_empty() const;
        size_t size() const;

        size_t first() const;
        size_t last() const;

        Operation& operator[](size_t index);
        Operation& front();
        Operation& back();

        std::pair<OperationSlice, OperationSlice> split(size_t index);

    private:
        std::span<OperationBatch> array_;
        size_t                    first_;
        size_t                    last_;
    };

    struct OperationShufflerMetrics {
        size_t completed_task_count = 0;

        struct {
            size_t max_depth          = 0;
            size_t min_partition_size = 0;
        } canceled_task_count;
    };

    // This class incrementally sorts a collection of operations to put them in order approximately by address.
    // The intention is to reduce TLB faults and improve cache utilization when the `Region` applies the operations.
    //
    // We can terminate early, before the array(s) have been totally sorted. Some possible heuristics:
    //   - Time elapsed.
    //   - The first/last operation in a given partition live on the same page as a quick check for how sorted things are.
    //      - This makes assumptions about the address distribution.
    //   - Size of the partition. We use quick-sort internally so we shouldn't let this get too small.
    //      - Could switch to insertion sort when things get small.
    //
    class OperationShuffler {
    public:
        using Metrics = OperationShufflerMetrics;

        OperationShuffler();

        const Metrics& metrics() const;

        size_t max_depth() const;
        void set_max_depth(size_t value);

        size_t min_partition_size() const;
        void set_min_partition_size(size_t value);

        size_t run(size_t max_step_count = std::numeric_limits<size_t>::max());
        bool step();

        // Adds a task to sort these operations in place, incrementally.
        void sort(std::span<OperationBatch> operations);
        void sort(OperationSlice operations);

        // Remove all previously added tasks, ready or not.
        void clear();

    private:
        struct SortTask {
            OperationSlice slice;
            size_t         depth;

            std::pair<SortTask, SortTask> fork(size_t pivot_index);

            // This is used to schedule tasks based on the amount of remaining work (size).
            bool operator<(const SortTask& that) const;
        };

        void add_task(SortTask task);
        std::optional<SortTask> take_task();

    private:
        static constexpr size_t MAX_DEPTH_CEILING        = std::numeric_limits<size_t>::max();
        static constexpr size_t MIN_PARTITION_SIZE_FLOOR = 2;

        Metrics               metrics_;
        size_t                max_depth_;
        size_t                min_partition_size_;
        std::vector<SortTask> task_heap_;
    };

}
