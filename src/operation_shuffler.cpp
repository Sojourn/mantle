#include "mantle/operation_shuffler.h"
#include <algorithm>
#include <utility>
#include <cassert>

namespace mantle {

    static size_t partition(OperationSlice slice) {
        // FIXME: This is a terrible choice. Look into Hoare or three-way partitioning.
        Operation pivot = slice.back();

        size_t i = 0;
        for (size_t j = 0; j < slice.size() - 1; ++j) {
            if (slice[j] < pivot) {
                std::swap(slice[i], slice[j]);
                ++i;
            }
        }

        std::swap(slice[i], slice.back());

        return i;
    }

    OperationSlice::OperationSlice()
        : first_(0)
        , last_(0)
    {
    }

    OperationSlice::OperationSlice(std::span<OperationBatch> array)
        : array_(array)
        , first_(0)
        , last_(array.size() * OperationBatch::SIZE)
    {
    }

    OperationSlice::OperationSlice(std::span<OperationBatch> array, size_t first, size_t last)
        : array_(array)
        , first_(first)
        , last_(last)
    {
        assert(first_ <= last_);
        assert(last_ <= (array.size() * OperationBatch::SIZE));
    }

    bool OperationSlice::is_empty() const {
        return last_ == first_;
    }

    size_t OperationSlice::size() const {
        return last_ - first_;
    }

    size_t OperationSlice::first() const {
        return first_;
    }

    size_t OperationSlice::last() const {
        return last_;
    }

    Operation& OperationSlice::operator[](size_t index) {
        size_t unsigned_index = static_cast<size_t>(first_ + index);
        return array_[unsigned_index / OperationBatch::SIZE].operations[unsigned_index % OperationBatch::SIZE];
    }

    Operation& OperationSlice::front() {
        return (*this)[0];
    }

    Operation& OperationSlice::back() {
        return (*this)[size() - 1];
    }

    std::pair<OperationSlice, OperationSlice> OperationSlice::split(size_t index) {
        assert(index < size());

        return {
            OperationSlice(array_, first_, first_ + index),
            OperationSlice(array_, first_ + index + 1, last_),
        };
    }

    OperationShuffler::OperationShuffler()
        : max_depth_(MAX_DEPTH_CEILING)
        , min_partition_size_(MIN_PARTITION_SIZE_FLOOR)
    {
    }

    auto OperationShuffler::metrics() const -> const Metrics& {
        return metrics_;
    }

    void OperationShuffler::set_max_depth(size_t value) {
        max_depth_ = std::min(value, MAX_DEPTH_CEILING);
    }

    void OperationShuffler::set_min_partition_size(size_t value) {
        min_partition_size_ = std::max(value, MIN_PARTITION_SIZE_FLOOR);
    }

    size_t OperationShuffler::run(size_t max_step_count) {
        for (size_t step_count = 0; step_count < max_step_count; ++step_count) {
            if (!step()) {
                return step_count;
            }
        }

        return max_step_count;
    }

    bool OperationShuffler::step() {
        while (std::optional<SortTask> task = take_task()) {
            if (task->slice.size() < min_partition_size_) {
                metrics_.canceled_task_count.min_partition_size += 1;
                continue;
            }
            if (task->depth > max_depth_) {
                metrics_.canceled_task_count.max_depth += 1;
                continue;
            }

            auto&& [l_subtask, r_subtask] = task->fork(
                partition(task->slice)
            );

            add_task(l_subtask);
            add_task(r_subtask);

            metrics_.completed_task_count += 1;
            return true;
        }

        return false;
    }

    void OperationShuffler::sort(std::span<OperationBatch> operations) {
        sort(OperationSlice(operations));
    }

    void OperationShuffler::sort(OperationSlice operations) {
        add_task(
            SortTask {
                .slice = operations,
                .depth = 0,
            }
        );
    }

    void OperationShuffler::clear() {
        task_heap_.clear();
    }

    auto OperationShuffler::SortTask::fork(size_t pivot_index) -> std::pair<SortTask, SortTask> {
        auto&& [l_slice, r_slice] = slice.split(pivot_index);

        return {
            SortTask {
                .slice = l_slice,
                .depth = depth + 1,
            },
            SortTask {
                .slice = r_slice,
                .depth = depth + 1,
            },
        };
    }

    bool OperationShuffler::SortTask::operator<(const SortTask& that) const {
        return slice.size() < that.slice.size();
    }

    void OperationShuffler::add_task(SortTask task) {
        task_heap_.push_back(task);

        std::push_heap(task_heap_.begin(), task_heap_.end());
    }

    auto OperationShuffler::take_task() -> std::optional<SortTask> {
        if (task_heap_.empty()) {
            return std::nullopt;
        }

        std::pop_heap(task_heap_.begin(), task_heap_.end());

        std::optional<SortTask> task = task_heap_.back();
        task_heap_.pop_back();
        return task;
    }

}
