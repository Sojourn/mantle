#include "mantle/mantle.h"

using namespace mantle;

class CommonObjectFinalizer final : public Finalizer {
public:
    bool delete_objects = true;
    size_t count = 0;

    void finalize(ObjectGroup, std::span<Object*> objects) noexcept override {
        count += objects.size();

        if (delete_objects) {
            for (Object* object : objects) {
                delete object;
            }
        }
    }
};

inline Ref<Object> make_object() {
    return bind(*new Object);
}
