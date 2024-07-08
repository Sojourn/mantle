#include "catch.hpp"
#include "mantle/mantle.h"

using namespace mantle;

class BasicObjectFinalizer final : public ObjectFinalizer {
public:
    size_t count = 0;

    void finalize(ObjectGroup, std::span<Object*> objects) noexcept override {
        count += objects.size();

        for (Object* object : objects) {
            delete object;
        }
    }
};

TEST_CASE("Ref") {
    BasicObjectFinalizer finalizer;

    auto make_object_ref = []() {
        return bind(*(new Object));
    };

    SECTION("Simple") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ref<Object> ref = make_object_ref();
                (void)ref;
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }
}
