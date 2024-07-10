#pragma once

#include <iostream>
#include <span>
#include <array>
#include <atomic>
#include <limits>
#include <cstdint>
#include <cstddef>
#include <climits>
#include <cassert>
#include "mantle/config.h"

namespace mantle {

    class Object;

    using RegionId        = uint16_t;
    using ObjectGroup     = uint16_t;
    using ObjectGroupMask = std::array<uint64_t, std::numeric_limits<ObjectGroup>::max() / (sizeof(uint64_t) * CHAR_BIT)>;
    using AtomicSequence  = std::atomic_uint64_t;
    using Sequence        = AtomicSequence::value_type;

    // TODO: Move this into a separate file. It knows too much aabout other classes.
    struct ObjectGroups {
        Object**         objects;
        size_t           object_count;
        ObjectGroup      group_min;     // Inclusive.
        ObjectGroup      group_max;     // Inclusive.
        size_t*          group_offsets; // Offsets into the objects array (where to find members).
        ObjectGroupMask* group_mask;    // A bitset of non-empty groups.

        [[nodiscard]]
        size_t group_member_count(ObjectGroup group) const {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            return group_offsets[static_cast<size_t>(group) + 1] - group_offsets[group];
        }

        [[nodiscard]]
        std::span<Object*> group_members(ObjectGroup group) {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            const size_t offset = group_offsets[static_cast<size_t>(group)];
            const size_t length = group_member_count(group);

            return {
                &objects[offset],
                length
            };
        }

        template<typename Visitor>
        void for_each_group(Visitor&& visitor) {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            for (ObjectGroup group = group_min; group <= group_max; ++group) {
                if (std::span<Object*> members = group_members(group); !members.empty()) {
                    visitor(group, members);
                }
            }
        }
    };

    static constexpr RegionId INVALID_REGION_ID = std::numeric_limits<RegionId>::max();

}
