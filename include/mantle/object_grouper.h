#pragma once

#include "mantle/types.h"
#include "mantle/object.h"
#include <cstdint>
#include <cstddef>
#include <cassert>

namespace mantle {

    // This class groups objects for more efficient finalization.
    class ObjectGrouper {
    public:
        ObjectGrouper() {
            reset();
        }

        void write(Object& object) {
            const ObjectGroup group = object.group();
            input_.push_back(&object);

            group_buckets_[group] += 1;
            group_min_ = std::min(group, group_min_);
            group_max_ = std::max(group, group_max_);
        }

        [[nodiscard]]
        ObjectGroups flush() {
            output_.resize(input_.size());

            // Calculate group offsets and initialize the group mask.
            {
                size_t offset = 0;
                for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                    size_t group_size = group_buckets_[group];
                    uint64_t group_populated = !!group_size;

                    group_offsets_[group] = offset;
                    group_mask_[group / 64] |= (group_populated << (group % 64));

                    offset += group_size;
                }

                // The cumulative offset is stored at the end (not the back).
                assert(offset == input_.size());
                group_offsets_[static_cast<size_t>(group_max_) + 1] = offset;
            }

            // Group objects in O(n) using radix sort.
            for (size_t i = 0; i < input_.size(); ++i) {
                Object* object = input_[i];
                ObjectGroup group = object->group();

                size_t offset = group_offsets_[group];
                size_t& bucket = group_buckets_[group];
                assert(bucket);

                bucket -= 1;
                output_[offset + bucket] = object;
            }

            return {
                .objects       = output_.data(),
                .group_min     = group_min_,
                .group_max     = group_max_,
                .group_offsets = group_offsets_.data(),
                .group_mask    = &group_mask_,
            };
        }

        void reset() {
            input_.clear();
            output_.clear();
            group_min_ = std::numeric_limits<ObjectGroup>::max();
            group_max_ = std::numeric_limits<ObjectGroup>::min();

            for (size_t& bucket: group_buckets_) {
                bucket = 0;
            }
            for (size_t& offset: group_offsets_) {
                offset = 0;
            }
            for (uint64_t& chunk: group_mask_) {
                chunk = 0;
            }
        }

    private:
        void calculate_offsets() {
            size_t offset = 0;
            for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                group_offsets_[group] = offset;
                offset += group_buckets_[group];
            }

            assert(offset == input_.size());
            group_offsets_.back() = input_.size();
        }

    private:
        using GroupBucketArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 0>;
        using GroupOffsetArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 1>;

        std::vector<Object*> input_;
        std::vector<Object*> output_;
        ObjectGroup          group_min_;
        ObjectGroup          group_max_;
        GroupOffsetArray     group_offsets_;
        GroupBucketArray     group_buckets_;
        ObjectGroupMask      group_mask_;
    };

}
