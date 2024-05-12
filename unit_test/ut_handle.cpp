#include "catch.hpp"
#include "mantle/mantle.h"
#include "mantle/debug.h"

using namespace mantle;

struct TestObject : Object {
    size_t birth_count = 0;
    size_t death_count = 0;
};

// TestObjectAllocator?
class TestObjectFinalizer : public ObjectFinalizer {
public:
    TestObjectFinalizer(std::vector<TestObject*>& pool)
        : pool_(pool)
        , count_(0)
    {
    }

    size_t count() const {
        return count_;
    }

    void finalize(Object& object) noexcept override final {
        TestObject& test_object = static_cast<TestObject&>(object);
        test_object.death_count += 1;
        pool_.push_back(&test_object);
        count_++;

        CHECK(test_object.birth_count == test_object.death_count);
    }

private:
    std::vector<TestObject*>& pool_;
    size_t                    count_;
};

TEST_CASE("Handle") {
    std::array<TestObject, 16> storage;

    std::vector<TestObject*> pool;
    for (TestObject& test_object: storage) {
        pool.push_back(&test_object);
    }

    auto new_test_object = [&]() -> Handle<TestObject> {
        if (pool.empty()) {
            return nullptr;
        }

        TestObject* test_object = pool.back();
        test_object->birth_count += 1;
        pool.pop_back();

        return make_handle(*test_object);
    };

    SECTION("Null") {
        TestObjectFinalizer finalizer(pool);
        {
            Domain domain;
            Region region(domain, finalizer);

            {
                Handle<TestObject> h0;
                Handle<TestObject> h1 = std::move(h0);
                Handle<TestObject> h2 = h1;

                (void)h2;
            }
            CHECK(finalizer.count() == 0);
        }
        CHECK(finalizer.count() == 0);
    }

    SECTION("Unique ownership") {
        TestObjectFinalizer finalizer(pool);
        {
            Domain domain;
            Region region(domain, finalizer);
            {
                Handle<TestObject> h0 = new_test_object();
                Handle<TestObject> h1 = std::move(h0);
                Handle<TestObject> h2;
                h2 = std::move(h1);
            }
            CHECK(finalizer.count() == 0);
        }
        CHECK(finalizer.count() == 1);
    }

    SECTION("Weight") {
        SECTION("Split") {
            TestObjectFinalizer finalizer(pool);
            {
                Domain domain;
                Region region(domain, finalizer);
                {
                    Handle<TestObject> h0 = new_test_object();
                    CHECK(h0.weight() == 0);
                }
            }
        }

        SECTION("Clone") {
            TestObjectFinalizer finalizer(pool);
            {
                Domain domain;
                Region region(domain, finalizer);
                {
                    Handle<TestObject> h0 = new_test_object();

                    Handle<TestObject> h1 = h0;
                    CHECK(h0.weight() == Operation::EXPONENT_MAX - 1);
                    CHECK(h1.weight() == Operation::EXPONENT_MAX - 1);

                    // Exhaust the weight of h0.
                    do {
                        h1 = h0;
                        h1.reset();
                    } while (h0.weight());

                    // Do one more copy from it, forcing it to gain weight and split.
                    h1 = h0;
                    CHECK(h0.weight() == Operation::EXPONENT_MAX - 1);
                    CHECK(h1.weight() == Operation::EXPONENT_MAX - 1);
                }
            }
        }
    }

    SECTION("Shared ownership") {
        TestObjectFinalizer finalizer(pool);
        {
            Domain domain;
            Region region(domain, finalizer);
            {
                Handle<TestObject> h0 = new_test_object();
                CHECK(h0.weight() == 0);

                Handle<TestObject> h1 = h0;
                CHECK(h0.weight() == (Operation::EXPONENT_MAX - 1));
                CHECK(h1.weight() == (Operation::EXPONENT_MAX - 1));

                Handle<TestObject> h2;
                h2 = h1;
                CHECK(h1.weight() == (Operation::EXPONENT_MAX - 2));
                CHECK(h2.weight() == (Operation::EXPONENT_MAX - 2));

                h0.reset();
                CHECK(h0.weight() == 0);

                h0 = std::move(h1);
                CHECK(h0.weight() == (Operation::EXPONENT_MAX - 2));
                CHECK(h1.weight() == 0);

                h1 = h2;
                CHECK(h1.weight() == (Operation::EXPONENT_MAX - 3));
                CHECK(h2.weight() == (Operation::EXPONENT_MAX - 3));

                h2 = h2;
                CHECK(h2.weight() == (Operation::EXPONENT_MAX - 3));

                h1 = h1;
                CHECK(h1.weight() == (Operation::EXPONENT_MAX - 3));
            }
            CHECK(finalizer.count() == 0);
        }
        CHECK(finalizer.count() == 1);
    }
}
