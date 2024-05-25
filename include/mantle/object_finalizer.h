#pragma once

#include "mantle/types.h"

namespace mantle {

    // An interface for cleaning up objects once they are no longer referenced.
    class ObjectFinalizer {
    public:
        virtual ~ObjectFinalizer() = default;

        // Objects are finalized in batches based on group membership.
        virtual void finalize(ObjectGroup group, std::span<Object*> objects) noexcept = 0;
    };

}
