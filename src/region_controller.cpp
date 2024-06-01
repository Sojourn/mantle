#include "mantle/region_controller.h"
#include "mantle/region.h"
#include "mantle/object.h"
#include "mantle/config.h"
#include "mantle/debug.h"
#include <limits>
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <cassert>

namespace mantle {

    static RegionControllerAction to_action(RegionControllerPhase phase) {
        switch (phase) {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION)      \
            case RegionControllerPhase::MANTLE_REGION_CONTROLLER_PHASE: {       \
                return RegionControllerAction::MANTLE_REGION_CONTROLLER_ACTION; \
            }                                                                   \

            MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
        }

        abort(); // Unreachable.
    }

    static RegionControllerPhase next(RegionControllerPhase phase) {
        return static_cast<RegionControllerPhase>(
            (static_cast<size_t>(phase) + 1) % REGION_CONTROLLER_PHASE_COUNT
        );
    }

    RegionControllerCensus::RegionControllerCensus()
        : count_(0)
        , min_cycle_(std::numeric_limits<Cycle>::max())
        , max_cycle_(std::numeric_limits<Cycle>::min())
    {
        for (size_t& counter: state_counts_) {
            counter = 0;
        }
        for (size_t& counter: phase_counts_) {
            counter = 0;
        }
        for (size_t& counter: action_counts_) {
            counter = 0;
        }
    }

    RegionControllerCensus::RegionControllerCensus(const RegionControllerGroup& controllers)
        : RegionControllerCensus()
    {
        add(controllers);
    }

    void RegionControllerCensus::add(const RegionController& controller) {
        count_ += 1;
        min_cycle_ = std::min(controller.cycle(), min_cycle_);
        max_cycle_ = std::max(controller.cycle(), max_cycle_);
        state_counts_[static_cast<size_t>(controller.state())] += 1;
        phase_counts_[static_cast<size_t>(controller.phase())] += 1;
        action_counts_[static_cast<size_t>(controller.action())] += 1;
    }

    void RegionControllerCensus::add(const RegionControllerGroup& controllers) {
        for (const std::unique_ptr<RegionController>& controller: controllers) {
            add(*controller);
        }
    }

    size_t RegionControllerCensus::count() const {
        return count_;
    }

    auto RegionControllerCensus::min_cycle() const -> Cycle {
        return min_cycle_;
    }

    auto RegionControllerCensus::max_cycle() const -> Cycle {
        return max_cycle_;
    }

    bool RegionControllerCensus::any(State state) const {
        return state_counts_[static_cast<size_t>(state)] != 0;
    }

    bool RegionControllerCensus::all(State state) const {
        return (count_ > 0) && state_counts_[static_cast<size_t>(state)] == count_;
    }

    bool RegionControllerCensus::any(Phase phase) const {
        return phase_counts_[static_cast<size_t>(phase)] != 0;
    }

    bool RegionControllerCensus::all(Phase phase) const {
        return (count_ > 0) && phase_counts_[static_cast<size_t>(phase)] == count_;
    }

    bool RegionControllerCensus::any(Action action) const {
        return action_counts_[static_cast<size_t>(action)] != 0;
    }

    bool RegionControllerCensus::all(Action action) const {
        return (count_ > 0) && action_counts_[static_cast<size_t>(action)] == count_;
    }

    RegionController::RegionController(
        RegionId region_id,
        RegionControllerGroup& controllers,
        const OperationLedger& ledger,
        const Config& config
    )
        : region_id_(region_id)
        , controllers_(controllers)
        , ledger_(ledger)
        , config_(config)
        , state_(State::STARTING)
        , phase_(Phase::START)
        , cycle_(0)
        , operation_grouper_(retired_increments_, retired_decrements_)
        , metrics_(operation_grouper_)
    {
    }

    RegionId RegionController::region_id() const {
        return region_id_;
    }

    auto RegionController::metrics() const -> const Metrics& {
        return metrics_;
    }

    bool RegionController::is_quiescent() const {
        return !operation_grouper_.is_dirty();
    }

    auto RegionController::state() const -> State {
        return state_;
    }

    auto RegionController::phase() const -> Phase {
        return phase_;
    }

    auto RegionController::cycle() const -> Cycle {
        return cycle_;
    }

    auto RegionController::action() const -> Action {
        return to_action(phase_);
    }

    void RegionController::start(Cycle cycle) {
        if (state_ != State::STARTING) {
            assert(false);
            return;
        }

        transition(cycle);
        transition(State::RUNNING);
    }

    void RegionController::stop() {
        if (state_ != State::STOPPING) {
            assert(false);
            return;
        }

        transition(State::STOPPED);
    }

    auto RegionController::send_message() -> std::optional<Message> {
        switch (phase_) {
            case Phase::START: {
                break;
            }
            case Phase::START_BARRIER: {
                break;
            }
            case Phase::ENTER: {
                transition(Phase::SUBMIT);

                return Message {
                    .enter = {
                        .type  = MessageType::ENTER,
                        .cycle = cycle_,
                    },
                };
            }
            case Phase::SUBMIT: {
                break;
            }
            case Phase::SUBMIT_BARRIER: {
                break;
            }
            case Phase::RETIRE_BARRIER: {
                break;
            }
            case Phase::RETIRE: {
                transition(Phase::LEAVE);

                return Message {
                    .retire = {
                        .type    = MessageType::RETIRE,
                        .garbage = object_grouper_.flush(),
                    },
                };
            }
            case Phase::LEAVE: {
                transition(Phase::START);
                if (state_ == State::STOPPED) {
                    transition(State::SHUTDOWN);
                }

                return Message {
                    .leave = {
                        .type = MessageType::LEAVE,
                        .stop = state_ == State::SHUTDOWN,
                    },
                };
            }
        }

        return std::nullopt;
    }

    void RegionController::receive_message(const Message& message) {
        switch (phase_) {
            case Phase::START: {
                if (message.type == MessageType::START) {
                    transition(Phase::START_BARRIER);
                }
                break;
            }
            case Phase::START_BARRIER: {
                break; // Redundant start messages are dropped.
            }
            case Phase::ENTER: {
                break; // Redundant start messages are dropped.
            }
            case Phase::SUBMIT: {
                if (message.type == MessageType::SUBMIT) {
                    transition(Phase::SUBMIT_BARRIER);

                    if (message.submit.stop) {
                        if (state_ == State::STOPPED) {
                            // The region is reaffirming that wants to stop.
                        }
                        else {
                            transition(State::STOPPING);
                        }
                    }
                    else {
                        // The region is not quiescent anymore. We should
                        // stop shutting down until it is again.
                        transition(State::RUNNING);
                    }

                    submitted_increments_ = message.submit.increments;
                    submitted_decrements_ = message.submit.decrements;
                }
                break; // Redundant start messages are dropped.
            }
            case Phase::SUBMIT_BARRIER: {
                break;
            }
            case Phase::RETIRE_BARRIER: {
                break;
            }
            case Phase::RETIRE: {
                break;
            }
            case Phase::LEAVE: {
                break;
            }
        }
    }

    void RegionController::synchronize(const RegionControllerCensus& census) {
        Phase next_phase = next(phase_);
        Action next_action = to_action(next_phase);

        if (census.all(Action::BARRIER_ALL) || census.all(Action::BARRIER_ANY)) {
            // Sanity check that the cycle matches while synchronized.
            assert(census.min_cycle() == census.max_cycle());

            transition(next_phase);
        }
        else if (census.any(next_phase) && (next_action == Action::BARRIER_ANY)) {
            transition(next_phase);
        }
    }

    void RegionController::transition(State next_state) {
        if (state_ == next_state) {
            return;
        }

        debug("[region_controller:{}] transition state {} to {}", region_id_, to_string(state_), to_string(next_state));
        state_ = next_state;
    }

    void RegionController::transition(Phase next_phase) {
        if (phase_ == next_phase) {
            return;
        }

        switch (phase_) {
            case Phase::START: {
                // Some region asked the domain to start a coherence cycle.
                break;
            }
            case Phase::START_BARRIER: {
                // All controllers have started.
                break;
            }
            case Phase::ENTER: {
                // Ask the region to submit operations.
                break;
            }
            case Phase::SUBMIT: {
                // The region has submitted operations for us to route.
                break;
            }
            case Phase::SUBMIT_BARRIER: {
                // We are waiting for all regions to respond.
                // NOTE: This phase can be combined with the next if we double buffer retired operations.
                metrics_.increment_count += route_operations(
                    OperationType::INCREMENT,
                    submitted_increments_
                );
                metrics_.decrement_count += route_operations(
                    OperationType::DECREMENT,
                    submitted_decrements_
                );
                break;
            }
            case Phase::RETIRE_BARRIER: {
                // All submitted operations have been routed.
                const bool force = (state_ == State::STOPPING) || (state_ == State::STOPPED);
                operation_grouper_.flush(force);

                // Apply flushed operations. Apply the increments first.
                // Note sure if this is needed, but better safe than sorry.
                apply_operations<OperationType::INCREMENT>(retired_increments_.span());
                apply_operations<OperationType::DECREMENT>(retired_decrements_.span());

                retired_increments_.clear();
                retired_decrements_.clear();
                break;
            }
            case Phase::RETIRE: {
                break;
            }
            case Phase::LEAVE: {
                transition(cycle_ + 1);
                break;
            }
        }

        debug("[region_controller:{}] transition phase {} to {}", region_id_, to_string(phase_), to_string(next_phase));
        phase_ = next_phase;
    }

    void RegionController::transition(Cycle next_cycle) {
        if (cycle_ == next_cycle) {
            return;
        }

        debug("[region_controller:{}] transition cycle {} to {}", region_id_, cycle_, next_cycle);
        cycle_ = next_cycle;
    }

    size_t RegionController::route_operations(OperationType type, SequenceRange range) {
        size_t count = 0;

        for (Sequence sequence = range.head; sequence != range.tail; ++sequence) {
            Operation operation = ledger_.read(sequence);
            if (type != operation.type()) {
                continue;
            }

            const Object* object = operation.object();
            if (!object) {
                continue;
            }

            RegionController& controller = *controllers_[object->region_id()];
            bool flush = !controller.config_.operation_grouper_enabled;
            controller.operation_grouper_.write(operation, flush);

            count += 1;
        }

        return count;
    }

    template<OperationType type>
    void RegionController::apply_operations(std::span<OperationBatch> operations) {
        for (const OperationBatch& batch: operations) {
            for (Operation operation: batch.operations) {
                if (Object* object = operation.mutable_object()) {
                    if (object->apply_operation<type>(operation)) {
                        // The object is still alive.
                    }
                    else {
                        object_grouper_.write(*object);
                    }
                }
                else {
                    // Ignore padding operations.
                }
            }
        }
    }

    RegionControllerCensus synchronize(RegionControllerGroup& controllers) {
        RegionControllerCensus old_census;
        RegionControllerCensus new_census;

        // Synchronizing until we can no longer make forward progress without sending or receiving.
        do {
            old_census = RegionControllerCensus(controllers);

            for (auto&& controller: controllers) {
                controller->synchronize(old_census);
            }

            new_census = RegionControllerCensus(controllers);
        } while (old_census != new_census);

        return new_census;
    }

    std::string_view to_string(RegionControllerState state) {
        using namespace std::literals;

        switch (state) {
#define X(MANTLE_REGION_CONTROLLER_STATE)                               \
            case RegionControllerState::MANTLE_REGION_CONTROLLER_STATE: \
                return #MANTLE_REGION_CONTROLLER_STATE ##sv;            \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
        }

        abort();
    }

    std::string_view to_string(RegionControllerPhase phase) {
        using namespace std::literals;

        switch (phase) {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
            case RegionControllerPhase::MANTLE_REGION_CONTROLLER_PHASE:    \
                return #MANTLE_REGION_CONTROLLER_PHASE ##sv;               \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
        }

        abort();
    }

    std::string_view to_string(RegionControllerAction action) {
        using namespace std::literals;

        switch (action) {
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                \
            case RegionControllerAction::MANTLE_REGION_CONTROLLER_ACTION: \
                return #MANTLE_REGION_CONTROLLER_ACTION ##sv;             \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
        }

        abort();
    }

}
