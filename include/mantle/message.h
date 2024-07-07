#pragma once

#include <string_view>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include "mantle/types.h"
#include "mantle/ledger.h"
#include "mantle/operation.h"

#define MANTLE_MESSAGE_TYPES(X) \
    X(START)                    \
    X(ENTER)                    \
    X(SUBMIT)                   \
    X(RETIRE)                   \
    X(LEAVE)                    \

namespace mantle {

    enum class MessageType {
#define X(MANTLE_MESSAGE_TYPE) \
        MANTLE_MESSAGE_TYPE,   \

        MANTLE_MESSAGE_TYPES(X)
#undef X
    };

    union Message {
        MessageType type;

        // region -> domain
        struct Start {
            MessageType type;
        } start;

        // domain -> region
        struct Enter {
            MessageType type;
            Sequence    cycle;
        } enter;

        // region -> domain
        struct Submit {
            MessageType   type;
            bool          stop; // The region is ready to stop.
            SequenceRange increments;
            SequenceRange decrements;

            // This will eventually replace increments and decrements.
            WriteBarrier* write_barrier;
        } submit;

        // domain -> region
        struct Retire {
            MessageType  type;
            ObjectGroups garbage;
        } retire;

        // domain -> region
        struct Leave {
            MessageType type;
            bool        stop; // The domain is ready to stop.
        } leave;
    };

    constexpr Message make_start_message() {
        return {
            .start = {
                .type = MessageType::START,
            }
        };
    }

    constexpr Message make_enter_message(Sequence cycle) {
        return {
            .enter = {
                .type  = MessageType::ENTER,
                .cycle = cycle,
            }
        };
    }

    constexpr Message make_leave_message(bool stop) {
        return {
            .leave = {
                .type = MessageType::LEAVE,
                .stop = stop,
            }
        };
    }

    constexpr std::string_view to_string(MessageType type) {
        using namespace std::literals;

        switch (type) {
#define X(MANTLE_MESSAGE_TYPE)                     \
            case MessageType::MANTLE_MESSAGE_TYPE: \
                return #MANTLE_MESSAGE_TYPE ##sv;  \

            MANTLE_MESSAGE_TYPES(X)
#undef X
        }

        abort();
    }

}
