#pragma once

#include <utility>
#include <type_traits>
#include "config.h"
#include "region.h"
#include "object.h"
#include "operation.h"

namespace mantle {

    // This class holds a strong reference to an Object derived class instance.
    // It implements a smart-pointer like interface and has semantics similar to std::shared_ptr.
    //
    // NOTE: We've got an extra `OperationType` bit we can use for a flag if needed.
    // NOTE: The exponent is never saturated at rest. We can use the high bit for a flag if needed.
    // TODO: Think about renaming this to `Ref<T>` for brevity.
    //
    template<typename T>
    class Handle {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        friend Handle<T> make_handle<T>(T& object) noexcept;

        // Bind an `Object` subclass to the local `Region` and return a managed `Handle` to it.
        static Handle bind(T& object) noexcept {
            static_cast<Object&>(object).bind(get_region().id());

            return Handle(make_decrement_operation(&object, Operation::EXPONENT_MIN));
        }

        explicit Handle(Operation operation) noexcept
            : operation_(operation)
        {
        }

    public:
        Handle() noexcept
            : operation_(make_null_operation())
        {
        }

        Handle(std::nullptr_t) noexcept
            : Handle()
        {
        }

        Handle(Handle&& other) noexcept
            : operation_(make_null_operation())
        {
            std::swap(operation_, other.operation_);
        }

        template<typename U>
        Handle(Handle<U>&& other) noexcept
            : operation_(make_null_operation())
        {
            std::swap(operation_, other.operation_);
        }

        Handle(const Handle& other) noexcept
            : Handle(other.copy_reference())
        {
        }

        template<typename U>
        Handle(const Handle<U>& other) noexcept
            : operation_(other.copy_reference())
        {
            static_assert(std::is_base_of_v<T, U>);
        }

        Handle& operator=(Handle&& that) noexcept {
            if (operation_.object() != that.operation_.object()) {
                reset();
                std::swap(operation_, that.operation_);
            }

            return *this;
        }

        template<typename U>
        Handle& operator=(Handle<U>&& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            if (operation_.object() != that.operation_.object()) {
                reset();
                std::swap(operation_, that.operation_);
            }

            return *this;
        }

        Handle& operator=(const Handle& that) noexcept {
            if (operation_.object() != that.operation_.object()) {
                reset();
                operation_ = that.copy_reference();
            }

            return *this;
        }

        template<typename U>
        Handle& operator=(const Handle<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            if (operation_.object() != that.operation_.object()) {
                reset();
                operation_ = that.copy_reference();
            }

            return *this;
        }

        ~Handle() noexcept {
            reset();
        }

        T* get() noexcept {
            return static_cast<T*>(operation_.mutable_object());
        }

        const T* get() const noexcept {
            return static_cast<const T*>(operation_.object());
        }

        T* operator->() noexcept {
            assert(*this);

            return get();
        }

        const T* operator->() const noexcept {
            assert(*this);

            return get();
        }

        T& operator*() noexcept {
            assert(*this);

            return *get();
        }

        const T& operator*() const noexcept {
            assert(*this);

            return *get();
        }

        explicit operator bool() const noexcept {
            return static_cast<bool>(operation_);
        }

        void reset() noexcept {
            if (operation_) {
                Object* object = operation_.mutable_object();
                assert(object);
                object->start_decrement_operation(operation_);

                operation_ = make_null_operation();
            }

            assert(!operation_);
        }

    public:
        uint8_t weight() const {
            return operation_.exponent();
        }

    private:
        [[nodiscard]] MANTLE_HOT Operation copy_reference() const {
            Object* object = operation_.mutable_object();
            if (!object) {
                return make_null_operation();
            }

            if constexpr (ENABLE_WEIGHTED_REFERENCE_COUNTING) {
                // Check if we need to gain additional weight.
                if (weight() == 0) {
                    // NOTE: Submit the new operation before the old operation.
                    object->start_increment_operation(make_increment_operation(object, Operation::EXPONENT_MAX));
                    object->start_decrement_operation(operation_);
                    operation_ = make_decrement_operation(object, Operation::EXPONENT_MAX);
                }

                // Split our weight in half by reducing the exponent by one.
                operation_ = make_decrement_operation(object, weight() - 1);
                return operation_;
            }
            else {
                // Create a paired increment and decrement.
                Operation increment = make_increment_operation(object);
                Operation decrement = make_decrement_operation(object);

                // The increment can be started immediately.
                object->start_increment_operation(increment);

                // The decrement will be started once the new reference is dropped.
                return decrement;
            }
        }

    private:
        mutable Operation operation_;
    };

    template<typename T>
    inline Handle<T> make_handle(T& object) noexcept {
        return Handle<T>::bind(object);
    }

}
