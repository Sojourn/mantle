#include "catch.hpp"
#include "mantle/mantle.h"

using namespace mantle;

struct ObjectImpl : Object {};

class BasicObjectFinalizer final : public Finalizer {
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

    auto make_object_impl_ref = []() {
        return bind(*(new ObjectImpl));
    };

    SECTION("Simple") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ref<Object> ref = make_object_ref();
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }

    SECTION("Copying") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ref<Object> ref0 = make_object_ref();
                Ref<Object> ref1 = ref0;

                // Self assignments.
                ref0 = ref0;
                ref1 = ref1;

                std::swap(ref0, ref1);
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }

    SECTION("Moving") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ref<Object> ref0 = make_object_ref();
                Ref<Object> ref1 = std::move(ref0);

                ref0 = std::move(ref1);
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }

    SECTION("Reference collections") {
        constexpr size_t count = 1000 * 1000; // 1M

        {
            Domain domain;
            Region region(domain, finalizer);

            {
                std::vector<Ref<Object>> refs;
                refs.reserve(count);
                for (size_t i = 0; i < count; ++i) {
                    refs.push_back(make_object_ref());
                }
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == count);
    }

    SECTION("Conversions") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ref<ObjectImpl> impl_ref = make_object_impl_ref();
                Ref<Object> base_ref = impl_ref;

                Ptr<ObjectImpl> impl_ptr = impl_ref;
                Ptr<Object> base_ptr = impl_ptr;

                base_ptr = impl_ref;
                base_ptr = base_ref;

                impl_ref = impl_ptr;
                base_ref = base_ptr;
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }

    SECTION("Null pointers") {
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Ptr<ObjectImpl> ptr = make_object_impl_ref();
                Ptr<ObjectImpl> null_ptr;
            }
            CHECK(finalizer.count == 0);
        }
        CHECK(finalizer.count == 1);
    }

}
