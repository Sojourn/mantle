#pragma once

#include <memory>
#include <span>
#include <array>
#include <optional>
#include <string_view>
#include <vector>
#include <compare>
#include <cstdint>
#include <cstddef>
#include "mantle/types.h"
#include "mantle/util.h"
#include "mantle/message.h"
#include "mantle/ledger.h"
#include "mantle/object.h"
#include "mantle/object_grouper.h"
#include "mantle/operation.h"
#include "mantle/operation_grouper.h"

// Actions represents what is needed for the controller to advance.
//
// SEND:        Waiting to send a message to the associated `Region`.
// RECEIVE:     Waiting to receive a message from the associated `Region`.
// BARRIER_ANY: Any controller reaching this state causes all controllers to advance past it.
// BARRIER_ALL: All controllers must reach this state to advance past it.
//
#define MANTLE_REGION_CONTROLLER_ACTIONS(X) \
    X(SEND)                                 \
    X(RECEIVE)                              \
    X(BARRIER_ANY)                          \
    X(BARRIER_ALL)                          \

#define MANTLE_REGION_CONTROLLER_PHASES(X)  \
    X(START,          RECEIVE)              \
    X(START_BARRIER,  BARRIER_ANY)          \
    X(ENTER,          SEND)                 \
    X(SUBMIT,         RECEIVE)              \
    X(SUBMIT_BARRIER, BARRIER_ALL)          \
    X(RETIRE_BARRIER, BARRIER_ALL)          \
    X(RETIRE,         SEND)                 \
    X(LEAVE,          SEND)                 \

#define MANTLE_REGION_CONTROLLER_STATES(X) \
    X(STARTING)                            \
    X(RUNNING)                             \
    X(STOPPING)                            \
    X(STOPPED)                             \
    X(SHUTDOWN)                            \

namespace mantle {

    class Region;
    class RegionController;

    using RegionControllerGroup = std::vector<std::unique_ptr<RegionController>>;

    // What we are doing in this part of the cycle.
    enum class RegionControllerAction {
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                 \
        MANTLE_REGION_CONTROLLER_ACTION,                                   \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
    };

    // Which part of the cycle we are in.
    enum class RegionControllerPhase {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
        MANTLE_REGION_CONTROLLER_PHASE,                                    \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
    };

    // High level state related to starting and stopping.
    enum class RegionControllerState {
#define X(MANTLE_REGION_CONTROLLER_STATE) \
        MANTLE_REGION_CONTROLLER_STATE,   \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
    };

    constexpr size_t REGION_CONTROLLER_ACTION_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                 \
        + 1                                                                \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
    ;

    constexpr size_t REGION_CONTROLLER_PHASE_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
        + 1                                                                \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
    ;

    constexpr size_t REGION_CONTROLLER_STATE_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_STATE) \
        + 1                               \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
    ;

    // A survey of the states of controllers and the actions they are trying to take.
    class RegionControllerCensus {
    public:
        using State = RegionControllerState;
        using Phase = RegionControllerPhase;
        using Cycle = Sequence;
        using Action = RegionControllerAction;

        RegionControllerCensus();
        explicit RegionControllerCensus(const RegionController& controller);
        explicit RegionControllerCensus(const RegionControllerGroup& controllers);

        void add(const RegionController& controller);
        void add(const RegionControllerGroup& controllers);

        size_t count() const;

        Cycle min_cycle() const;
        Cycle max_cycle() const;

        bool any(State state) const;
        bool all(State state) const;

        bool any(Phase phase) const;
        bool all(Phase phase) const;
 
        bool any(Action action) const;
        bool all(Action action) const;

        auto operator<=>(const RegionControllerCensus&) const noexcept = default;

    private:
        size_t                                             count_;
        Cycle                                              min_cycle_;
        Cycle                                              max_cycle_;
        std::array<size_t, REGION_CONTROLLER_STATE_COUNT>  state_counts_;
        std::array<size_t, REGION_CONTROLLER_PHASE_COUNT>  phase_counts_;
        std::array<size_t, REGION_CONTROLLER_ACTION_COUNT> action_counts_;
    };

    struct RegionControllerMetrics {
        const OperationGrouper::Metrics& operation_grouper;
        const ObjectGrouper::Metrics& object_grouper;
        size_t increment_count;
        size_t decrement_count;

        RegionControllerMetrics(
            const OperationGrouper& operation_grouper,
            const ObjectGrouper& object_grouper
        )
            : operation_grouper(operation_grouper.metrics())
            , object_grouper(object_grouper.metrics())
            , increment_count(0)
            , decrement_count(0)
        {
        }
    };

    // The `Domain` creates one of these for each bound `Region`.
    // It is responsible for driving the synchronization mechanism
    // between the associated `Region` and peer `RegionController`'s.
    // Controllers can be in different states, and synchronize among
    // themselves at barrier states as needed.
    //
    class RegionController {
    public:
        using State   = RegionControllerState;
        using Phase   = RegionControllerPhase;
        using Cycle   = Sequence;
        using Action  = RegionControllerAction;
        using Metrics = RegionControllerMetrics;

        RegionController(
            RegionId region_id,
            RegionControllerGroup& controllers,
            WriteBarrierManager& write_barrier_manager
        );

        RegionController(RegionController&&) = delete;
        RegionController(const RegionController&) = delete;
        RegionController& operator=(RegionController&&) = delete;
        RegionController& operator=(const RegionController&) = delete;

        RegionId region_id() const;
        const Metrics& metrics() const;

        bool is_quiescent() const;

        State state() const;
        Phase phase() const;
        Cycle cycle() const;
        Action action() const;

        void start(Cycle cycle);
        void stop();

        std::optional<Message> send_message();
        void receive_message(const Message& message);
        void synchronize(const RegionControllerCensus& census);

    private:
        void transition(State next_state);
        void transition(Phase next_phase);
        void transition(Cycle next_cycle);

        size_t route_operations(OperationType type, std::span<Object*> objects);

    private:
        RegionId               region_id_;
        RegionControllerGroup& controllers_;
        WriteBarrierManager&   write_barrier_manager_;

        State                  state_;
        Phase                  phase_;
        Cycle                  cycle_;

        WriteBarrier*          write_barrier_;
        OperationGrouper       operation_grouper_;
        ObjectGrouper          object_grouper_;

        Metrics                metrics_;
    };

    // Synchronizes a group of region controllers.
    RegionControllerCensus synchronize(RegionControllerGroup& controllers);

    std::string_view to_string(RegionControllerState state);
    std::string_view to_string(RegionControllerPhase phase);
    std::string_view to_string(RegionControllerAction action);

}
