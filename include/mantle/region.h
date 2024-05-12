#pragma once

#include <span>
#include <string_view>
#include <utility>
#include "mantle/types.h"
#include "mantle/message.h"
#include "mantle/connection.h"
#include "mantle/operation.h"
#include "mantle/operation_ledger.h"

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
    class ObjectFinalizer;

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

        Region(Domain& domain, ObjectFinalizer& finalizer);
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
        template<typename T>
        friend class Handle;
        friend class Object;

        // TODO: Annotate these functions as `hot` and force them to inline.
        void start_increment_operation(Object& object, Operation operation);
        void start_decrement_operation(Object& object, Operation operation);

        // TODO: Annotate this function as `cold`.
        void flush_operation(Operation operation);

        // Called by an object when its reference count drops to zero.
        void handle_abandoned(Object& object);

    private:
        friend class Domain;

        const OperationLedger& ledger() const;

        Endpoint& domain_endpoint();
        Endpoint& region_endpoint();

    private:
        void handle_message(const Message& message);

        void transition(State next_state);
        void transition(Phase next_phase);
        void transition(Cycle next_cycle);

        template<OperationType type>
        void apply_operations(OperationRange operations);

    private:
        static constexpr State INITIAL_STATE = State::RUNNING;
        static constexpr Phase INITIAL_PHASE = Phase::RECV_ENTER;
        static constexpr Cycle INITIAL_CYCLE = 0;

        Domain&              domain_;
        RegionId             id_;

        State                state_;
        Phase                phase_;
        Cycle                cycle_;
        size_t               depth_;

        ObjectFinalizer&     finalizer_;
        OperationLedger      ledger_;
        std::vector<Object*> trash_;

        Connection           connection_;
    };

    std::string_view to_string(RegionState state);
    std::string_view to_string(RegionPhase phase);

    bool has_region();
    Region& get_region();

}
