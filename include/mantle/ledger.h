#pragma once

#include <cstddef>

namespace mantle {

    struct LedgerSegment {
        void**  objects;
        size_t  offset;
        size_t  increment_count;
        size_t  decrement_count;
        bool    increment_spill;
        bool    decrement_spill;
    };

    struct Ledger {
        static Ledger*& local_instance() {
            thread_local Ledger* instance = nullptr;
            return instance;
        }

        LedgerSegment* mutable_increment_segment;
        LedgerSegment* mutable_decrement_segment;
    };

    inline void write_increment(void* object) {
        LedgerSegment* segment = Ledger::local_instance()->mutable_increment_segment;
        segment->objects[segment->offset++] = object;
    }

    inline void write_decrement(void* object) {
        LedgerSegment* segment = Ledger::local_instance()->mutable_decrement_segment;
        segment->objects[segment->offset++] = object;
    }

    class Ref {
    public:
        explicit Ref(void* object) noexcept
            : object_(object)
        {
        }

        Ref(const Ref& that) noexcept
            : object_(that.object_)
        {
            write_increment(object_);
        }

        ~Ref() noexcept {
            write_decrement(object_);
        }

        Ref& operator=(const Ref& that) noexcept {
            if (this != &that) {
                write_decrement(object_);
                write_increment(that.object_);
                object_ = that.object_;
            }

            return *this;
        }

        void* object() const noexcept {
            return object_;
        }

    private:
        void* object_;
    };

}
