#include <vector>
#include <string>
#include <unordered_set>
#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include "mantle/mantle.h"

using namespace mantle;

#define MY_OBJECT_TYPES(X) \
    X(NUMBER)              \
    X(STRING)              \

enum class MyObjectType : uint16_t {
#define X(MY_OBJECT_TYPE) \
    MY_OBJECT_TYPE,       \

    MY_OBJECT_TYPES(X)
#undef X
};

template<MyObjectType type>
class MyObject;

template<>
class MyObject<MyObjectType::NUMBER> : public Object {
public:
    explicit MyObject(int64_t value)
        : Object(static_cast<ObjectGroup>(MyObjectType::NUMBER))
        , value_(value)
    {
    }

    int64_t value() const {
        return value_;
    }

private:
    int64_t value_;
};

template<>
class MyObject<MyObjectType::STRING> : public Object {
public:
    explicit MyObject(std::string value)
        : Object(static_cast<ObjectGroup>(MyObjectType::STRING))
        , value_(std::move(value))
    {
    }

    const std::string& value() const {
        return value_;
    }

private:
    std::string value_;
};

class MyFinalizer final : public Finalizer {
public:
    std::unordered_set<void*> dead_objects;

    void finalize(ObjectGroup group, std::span<Object*> objects) noexcept {
        for (Object* object : objects) {
            auto&& [_, inserted] = dead_objects.insert(object);
            assert(inserted);
        }

        // Delete an array containing all of the same type of object
        // to improve instruction cache utilization and reduce dispatch costs.
        switch (static_cast<MyObjectType>(group)) {
#define X(MY_OBJECT_TYPE)                                                                \
            case MyObjectType::MY_OBJECT_TYPE: {                                         \
                for (Object* object : objects) {                                         \
                    delete static_cast<MyObject<MyObjectType::MY_OBJECT_TYPE>*>(object); \
                }                                                                        \
                break;                                                                   \
            }                                                                            \

            MY_OBJECT_TYPES(X)
#undef X
        }
    }
};

using Number = MyObject<MyObjectType::NUMBER>;
using String = MyObject<MyObjectType::STRING>;

template<typename... Args>
Ref<Number> new_number(Args&&... args) {
    return bind_new<Number>(std::forward<Args>(args)...);
}

template<typename... Args>
Ref<String> new_string(Args&&... args) {
    return bind_new<String>(std::forward<Args>(args)...);
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    MyFinalizer finalizer;

    Domain domain;
    Region region(domain, finalizer);
    {
        Ref<String> string = new_string("Hello, World!");
        std::cout << string->value() << std::endl;

        std::vector<Ref<Number>> numbers;
        for (int64_t i = 0; i < 100000; ++i) {
            numbers.push_back(new_number(i));
        }

        std::cout << numbers.size() << std::endl;
    }
    region.stop();

    return EXIT_SUCCESS;
}
