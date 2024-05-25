#pragma once

#include <span>
#include <array>
#include <compare>
#include <atomic>
#include <limits>
#include <cstdint>
#include <cstddef>
#include <climits>

namespace mantle {

    class Object;

    using RegionId        = uint16_t;
    using ObjectGroup     = uint16_t;
    using ObjectGroupMask = std::array<uint64_t, std::numeric_limits<ObjectGroup>::max() / (sizeof(uint64_t) * CHAR_BIT)>;
    using AtomicSequence  = std::atomic_uint64_t;
    using Sequence        = AtomicSequence::value_type;

    struct SequenceRange {
        Sequence head;
        Sequence tail;

        constexpr size_t size() const {
            return tail - head;
        }

        constexpr auto operator<=>(const SequenceRange&) const noexcept = default;
    };


    struct ObjectGroups {
        Object**         objects;
        ObjectGroup      group_min;     // Inclusive.
        ObjectGroup      group_max;     // Inclusive.
        size_t*          group_offsets; // Offsets into the objects array (where to find members).
        ObjectGroupMask* group_mask;    // A bitset of non-empty groups.

        [[nodiscard]]
        size_t object_count() const {
            return group_offsets[static_cast<size_t>(group_max) + 1];
        }

        [[nodiscard]]
        size_t group_member_count(ObjectGroup group) const {
            return group_offsets[static_cast<size_t>(group) + 1] - group_offsets[group];
        }

        [[nodiscard]]
        std::span<Object*> group_members(ObjectGroup group) {
            return {
                &objects[group],
                group_member_count(group)
            };
        }

        template<typename Visitor>
        void for_each_group(Visitor&& visitor) {
            // TODO: Scan using the mask. This is probably faster for small group counts though...
            for (ObjectGroup group = group_min; group <= group_max; ++group) {
                visitor(group);
            }
        }
    };

    static constexpr RegionId INVALID_REGION_ID = std::numeric_limits<RegionId>::max();

}
