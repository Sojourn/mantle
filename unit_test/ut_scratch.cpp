#include "catch.hpp"
#include "mantle/mantle.h"
#include "mantle/debug.h"

using namespace mantle;

struct ScratchObject : Object {
    bool finalized = false;
};

class ScratchObjectFinalizer : public ObjectFinalizer {
public:
    ScratchObjectFinalizer(std::vector<ScratchObject*>& pool)
        : pool(pool)
    {
    }

    void finalize(ObjectGroup, std::span<Object*> objects) noexcept override final {
        for (Object* object: objects) {
            ScratchObject& scratch_object = static_cast<ScratchObject&>(*object);

            REQUIRE(!scratch_object.finalized);
            scratch_object.finalized = true;

            pool.push_back(&scratch_object);
        }
    }

    std::vector<ScratchObject*>& pool;
};

TEST_CASE("Scratch") {
    ScratchObject object;

    std::vector<ScratchObject*> pool;
    ScratchObjectFinalizer finalizer(pool);
    {
        Domain domain;
        Region region(domain, finalizer);
        {
            Handle<ScratchObject> h0 = make_handle(object);
            Handle<ScratchObject> h1 = std::move(h0);
            Handle<ScratchObject> h2;
            h2 = std::move(h1);
        }
        CHECK(finalizer.pool.empty());
    }
    CHECK(finalizer.pool.size() == 1);
}
