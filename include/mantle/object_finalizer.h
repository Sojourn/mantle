#pragma once

namespace mantle {

    class Object;

    // An interface for cleaning up objects once they
    // are no longer referenced.
    class ObjectFinalizer {
    public:
        virtual ~ObjectFinalizer() = default;

        virtual void finalize(Object& object) noexcept = 0;

        // TODO: Finalize batches of objects that have the same
        //       type to reduce the number of virtual calls.
        // virtual void finalize(uint16_t type, std::span<Object*> objects) noexcept = 0;
    };

}
