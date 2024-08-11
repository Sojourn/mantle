#pragma once

#include <stdexcept>
#include <type_traits>
#include "mantle/object.h"
#include "mantle/ledger.h"
#include "mantle/region.h"

namespace mantle {

    template<typename T>
    class Ref;

    template<typename T>
    class Ptr;

    template<typename T>
    Ref<T> bind(T& object) noexcept;

    template<typename T>
    Ptr<T> bind(T* object) noexcept;

    // A reference to an object that allows access and will keep it alive while at least
    // one reference exists to the object. References cannot be null (like normal C++ references).
    //
    template<typename T>
    class Ref {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        template<typename U>
        friend class Ref;

        template<typename U>
        friend class Ptr;

        friend Ref<T> bind<T>(T& object) noexcept;

        Ref(T& object)
            : object_(&object)
        {
            Region* region = Region::thread_local_instance();
            assert(region);

            static_cast<Object*>(object_)->bind(region->id());
        }

    public:
        Ref() = delete;

        Ref(const Ref& other) noexcept
            : object_(other.object_)
        {
            increment_ref_cnt(object_);
        }

        template<typename U>
        Ref(const Ref<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        template<typename U>
        Ref(const Ptr<U>& other);

        Ref& operator=(const Ref& that) noexcept {
            // We don't need to check if `this != that`.
            // The increment will be reordered before the decrement.
            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ref& operator=(const Ref<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ref& operator=(const Ptr<U>& that);

        ~Ref() noexcept {
            decrement_ref_cnt(object_);
        }

        T* get() noexcept {
            return object_;
        }

        const T* get() const noexcept {
            return object_;
        }

        T& operator*() noexcept {
            return *object_;
        }

        const T& operator*() const noexcept {
            return *object_;
        }

        T* operator->() noexcept {
            return object_;
        }

        const T* operator->() const noexcept {
            return object_;
        }

    private:
        T* object_;
    };

    // Pointers are nullable references. They also keep the object, pointed to alive (i.e. not weak).
    // This is in an improvement over `std::optional<Ref<T>>` in a couple of ways:
    //   1. `sizeof(Ptr<T>) < sizeof(std::optional<T>)`.
    //   2. More efficient copying (branchless).
    //   3. Automatic conversion to references with null checking.
    //
    template<typename T>
    class Ptr {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        template<typename U>
        friend class Ref;

        template<typename U>
        friend class Ptr;

        friend Ptr<T> bind<T>(T* object) noexcept;

        Ptr(T* object)
            : object_(object)
        {
            if (object_) {
                Region* region = Region::thread_local_instance();
                assert(region);

                static_cast<Object*>(object_)->bind(region->id());
            }
        }

    public:
        Ptr() noexcept
            : object_(nullptr)
        {
        }

        Ptr(const Ptr& other) noexcept
            : object_(other.object_)
        {
            increment_ref_cnt(object_);
        }

        template<typename U>
        Ptr(const Ptr<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        template<typename U>
        Ptr(const Ref<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        Ptr& operator=(const Ptr& that) noexcept {
            // We don't need to check if `this != that`.
            // The increment will be reordered before the decrement.
            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ptr& operator=(const Ptr<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ptr& operator=(const Ref<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        ~Ptr() noexcept {
            decrement_ref_cnt(object_);
        }

        T* get() noexcept {
            return object_;
        }

        const T* get() const noexcept {
            return object_;
        }

        T& operator*() noexcept {
            return *object_;
        }

        const T& operator*() const noexcept {
            return *object_;
        }

        T* operator->() noexcept {
            return object_;
        }

        const T* operator->() const noexcept {
            return object_;
        }

        explicit operator bool() const {
            return static_cast<bool>(object_);
        }

        void reset() {
            decrement_ref_cnt(std::exchange(object_, nullptr));
        }

    private:
        T* object_;
    };

    template<typename T>
    template<typename U>
    Ref<T>::Ref(const Ptr<U>& other)
        :  object_(other.object_)
    {
        static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.
        if (!object_) {
            throw std::runtime_error("Null reference");
        }

        increment_ref_cnt(object_);
    }

    template<typename T>
    template<typename U>
    Ref<T>& Ref<T>::operator=(const Ptr<U>& that) {
        static_assert(std::is_base_of_v<T, U>);
        if (!that.object_) {
            throw std::runtime_error("Null reference");
        }

        decrement_ref_cnt(object_);
        object_ = that.object_;
        increment_ref_cnt(object_);

        return *this;
    }

    template<typename T>
    inline Ref<T> bind(T& object) noexcept {
        return Ref<T>(object);
    }

    template<typename T>
    inline Ptr<T> bind(T* object) noexcept {
        return Ptr<T>(object);
    }

}

namespace std {

    // TODO: Specialize std::atomic to make it `is_lock_free`.
    // template<typename T>
    // class atomic<Ref<T>> {
    // public:
    // };
    // template<typename T>
    // class atomic<Ptr<T>> {
    // public:
    // };

}
