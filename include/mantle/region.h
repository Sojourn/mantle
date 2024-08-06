#pragma once

#include <span>
#include <optional>
#include <string_view>
#include <utility>
#include <cassert>
#include "mantle/types.h"
#include "mantle/util.h"
#include "mantle/ledger.h"
#include "mantle/message.h"
#include "mantle/connection.h"
#include "mantle/operation.h"

#define MANTLE_REGION_STATES(X) \
    X(RUNNING)                  \
    X(STOPPING)                 \
    X(STOPPED)                  \

#define MANTLE_REGION_PHASES(X) \
    X(RECV_ENTER)               \
    X(RECV_ENTER_SENT_START)    \
    X(RECV_RETIRE)              \
    X(RECV_LEAVE)               \

namespace mantle {

    class Domain;
    class Object;
    class Finalizer;

    enum class RegionState {
#define X(MANTLE_REGION_STATE) \
        MANTLE_REGION_STATE,   \

        MANTLE_REGION_STATES(X)
#undef X
    };

    enum class RegionPhase {
#define X(MANTLE_REGION_PHASE) \
        MANTLE_REGION_PHASE,   \

        MANTLE_REGION_PHASES(X)
#undef X
    };

    class Region {
    public:
        using State = RegionState;
        using Phase = RegionPhase;
        using Cycle = Sequence;

        Region(Domain& domain, Finalizer& finalizer);
        ~Region();

        Region(Region&&) = delete;
        Region(const Region&) = delete;
        Region& operator=(Region&&) = delete;
        Region& operator=(const Region&) = delete;

        RegionId id() const;

        State state() const;
        Phase phase() const;
        Cycle cycle() const;

        // Call step when this becomes readable.
        int file_descriptor();

        void stop();
        void step(bool non_blocking);

    private:
        friend class Domain;

        Endpoint& domain_endpoint();
        Endpoint& region_endpoint();

    private:
        void handle_message(const Message& message);

        void transition(State next_state);
        void transition(Phase next_phase);
        void transition(Cycle next_cycle);

        void finalize_garbage();

    private:
        static constexpr State INITIAL_STATE = State::RUNNING;
        static constexpr Phase INITIAL_PHASE = Phase::RECV_ENTER;
        static constexpr Cycle INITIAL_CYCLE = 0;

        Domain&                     domain_;
        RegionId                    id_;

        State                       state_;
        Phase                       phase_;
        Cycle                       cycle_;
        size_t                      depth_;

        Connection                  connection_;
        Finalizer&                  finalizer_;
        Ledger                      ledger_;
        std::optional<ObjectGroups> garbage_;

    public:
        static Region*& thread_local_instance() {
            thread_local Region* instance = nullptr;
            return instance;
        }
    };

    std::string_view to_string(RegionState state);
    std::string_view to_string(RegionPhase phase);

}
