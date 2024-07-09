#pragma once

#include "mantle/types.h"
#include "mantle/object.h"
#include <cstdint>
#include <cstddef>
#include <cassert>

namespace mantle {

    struct ObjectGrouperMetrics {
        size_t      object_count = 0;
        ObjectGroup group_min = std::numeric_limits<ObjectGroup>::max();
        ObjectGroup group_max = std::numeric_limits<ObjectGroup>::min();
    };

    // This class groups objects for more efficient finalization.
    class ObjectGrouper {
    public:
        using Metrics = ObjectGrouperMetrics;

        ObjectGrouper()
            : group_min_(std::numeric_limits<ObjectGroup>::max())
            , group_max_(std::numeric_limits<ObjectGroup>::min())
        {
            for (size_t& bucket: group_buckets_) {
                bucket = 0;
            }
        }

        [[nodiscard]]
        const Metrics& metrics() const {
            return metrics_;
        }

        void write(Object& object) {
            const ObjectGroup group = object.group();

            group_buckets_[group] += 1;
            group_min_ = std::min(group, group_min_);
            group_max_ = std::max(group, group_max_);

            input_.push_back(&object);
        }

        [[nodiscard]]
        ObjectGroups flush() {
            ObjectGroups groups = {};

            metrics_.object_count += input_.size();
            metrics_.group_min = std::min(group_min_, metrics_.group_min);
            metrics_.group_max = std::max(group_max_, metrics_.group_max);

            if constexpr (MANTLE_ENABLE_OBJECT_GROUPING) {
                // Reset working memory.
                output_.resize(input_.size());
                for (size_t& offset: group_offsets_) {
                    offset = 0;
                }
                for (uint64_t& chunk: group_mask_) {
                    chunk = 0;
                }

                // Calculate group offsets and initialize the group mask.
                {
                    size_t offset = 0;
                    for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                        const size_t group_size = group_buckets_[group];
                        const uint64_t group_populated = !!group_size;

                        group_offsets_[group] = offset;
                        group_mask_[group / 64] |= (group_populated << (group % 64));

                        offset += group_size;
                    }

                    // The cumulative offset is stored at the end (not the back).
                    assert(offset == input_.size());
                    group_offsets_[static_cast<size_t>(group_max_) + 1] = offset;
                }

                // Group objects in O(n) using radix sort.
                for (Object* object: input_) {
                    const ObjectGroup group = object->group();

                    const size_t offset = group_offsets_[group];
                    size_t& bucket = group_buckets_[group];
                    assert(bucket);

                    bucket -= 1;
                    output_[offset + bucket] = object;
                }

                groups = ObjectGroups {
                    .objects       = output_.data(),
                    .object_count  = output_.size(),
                    .group_min     = group_min_,
                    .group_max     = group_max_,
                    .group_offsets = group_offsets_.data(),
                    .group_mask    = &group_mask_,
                };

#ifdef MANTLE_AUDIT
                // Sanity check group membership.
                for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                    const std::span<Object*> group_members = groups.group_members(group);
                    assert(group_members.size() <= groups.object_count);

                    for (const Object* object: group_members) {
                        assert(object->group() == group);
                        assert(object->is_managed());
                    }
                }
#endif
            }
            else {
                output_ = input_;

                groups = ObjectGroups {
                    .objects       = output_.data(),
                    .object_count  = output_.size(),
                    .group_min     = group_min_,
                    .group_max     = group_max_,
                    .group_offsets = nullptr,
                    .group_mask    = nullptr,
                };
            }

            input_.clear();
            group_min_ = std::numeric_limits<ObjectGroup>::max();
            group_max_ = std::numeric_limits<ObjectGroup>::min();
            for (size_t& bucket: group_buckets_) {
                bucket = 0;
            }

            return groups;
        }

    private:
        using GroupBucketArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 0>;
        using GroupOffsetArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 1>;

        std::vector<Object*> input_;
        ObjectGroup          group_min_;
        ObjectGroup          group_max_;
        GroupBucketArray     group_buckets_;

        std::vector<Object*> output_;
        GroupOffsetArray     group_offsets_;
        ObjectGroupMask      group_mask_;

        Metrics              metrics_;
    };

}
