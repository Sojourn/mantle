#pragma once

#include <compare>
#include <atomic>
#include <limits>
#include <cstdint>
#include <cstddef>

namespace mantle {

    using AtomicSequence = std::atomic_uint64_t;
    using Sequence       = AtomicSequence::value_type;
    using RegionId       = uint16_t;

    struct SequenceRange {
        Sequence head;
        Sequence tail;

        constexpr size_t size() const {
            return tail - head;
        }

        constexpr auto operator<=>(const SequenceRange&) const noexcept = default;
    };

    static constexpr RegionId INVALID_REGION_ID = std::numeric_limits<RegionId>::max();

}
