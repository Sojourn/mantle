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
        : pool_(pool)
    {
    }

    void finalize(Object& object) noexcept override final {
        ScratchObject& scratch_object = static_cast<ScratchObject&>(object);

        REQUIRE(!scratch_object.finalized);
        scratch_object.finalized = true;

        pool_.push_back(&scratch_object);
    }

private:
    std::vector<ScratchObject*>& pool_;
};

TEST_CASE("Scratch") {
    ScratchObject object;
    std::vector<ScratchObject*> pool;
    pool.push_back(&object);

    SECTION("Initial Cache State") {
        using Cache = ObjectCache<int, 512, 8>;

        Cache cache;
        for (Cache::Cursor cursor; cursor; cursor.advance()) {
            Cache::Entry entry = cache.load(cursor);
            REQUIRE(entry.key == nullptr);
        }
    }
    SECTION("Reference Counting") {
        Domain domain;
        ScratchObjectFinalizer finalizer(pool);
        Region region(domain, finalizer);

        // SECTION("0") {
        //     Handle<ScratchObject> handle;
        // }
        SECTION("1") {
            Handle<ScratchObject> handle = make_handle(object);
        }
    }
}
