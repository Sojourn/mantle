#pragma once

namespace mantle {

    class Object;

    // An interface for cleaning up objects once they
    // are no longer referenced.
    class ObjectFinalizer {
    public:
        virtual ~ObjectFinalizer() = default;

        virtual void finalize(Object& object) noexcept = 0;
    };

}
