#pragma once

#include "mantle/mantle.h"

#define OBJECT_TYPES(X) \
    X(SOCKET)           \
    X(BUFFER)           \

enum class ObjectType : uint16_t {
#define X(OBJECT_TYPE) \
    OBJECT_TYPE,

    OBJECT_TYPES(X)
#undef X
};

template<ObjectType type>
class Object;

template<>
class Object<ObjectType::SOCKET> : public mantle::Object {
public:
    static constexpr ObjectType TYPE = ObjectType::SOCKET;

    Object()
        : mantle::Object(static_cast<std::underlying_type_t<ObjectType>>(TYPE))
    {
    }

    void send();
    void receive();
};

template<>
class Object<ObjectType::BUFFER> : public mantle::Object {
public:
    static constexpr ObjectType TYPE = ObjectType::BUFFER;

    Object()
        : mantle::Object(static_cast<std::underlying_type_t<ObjectType>>(TYPE))
    {
    }

    void assign(std::span<const std::byte> data);
    void append(std::span<const std::byte> data);

private:
    std::vector<std::byte> data_;
};
