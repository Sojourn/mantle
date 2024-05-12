#include "mantle/region.h"
#include "mantle/domain.h"
#include "mantle/object.h"
#include "mantle/object_finalizer.h"
#include "mantle/config.h"
#include "mantle/debug.h"
#include <cassert>

namespace mantle {

    thread_local Region* region_instance = nullptr;

    // Uses RAII to increment a counter on construction and decrement it on destruction.
    template<typename Counter>
    class [[nodiscard]] ScopedIncrement {
        ScopedIncrement(ScopedIncrement&&) = delete;
        ScopedIncrement(const ScopedIncrement&) = delete;
        ScopedIncrement& operator=(ScopedIncrement&&) = delete;
        ScopedIncrement& operator=(const ScopedIncrement&) = delete;

    public:
        explicit ScopedIncrement(Counter& counter)
            : counter_(counter)
        {
            counter_ += 1;
        }

        ~ScopedIncrement() {
            counter_ -= 1;
        }

    private:
        Counter& counter_;
    };

    Region::Region(Domain& domain, ObjectFinalizer& finalizer)
        : domain_(domain)
        , id_(std::numeric_limits<RegionId>::max())
        , state_(INITIAL_STATE)
        , phase_(INITIAL_PHASE)
        , cycle_(INITIAL_CYCLE)
        , depth_(0)
        , finalizer_(finalizer)
        , ledger_(domain.config().ledger_capacity)
    {
        // Register ourselves as the region on this thread.
        {
            if (region_instance) {
                throw std::runtime_error("Cannot have more than one region per thread");
            }

            region_instance = this;
        }

        // Synchronize with other regions until our cycle and phase match.
        {
            ledger_.begin_transaction();

            id_ = domain_.bind(*this);
            while (cycle_ == INITIAL_CYCLE) {
                bool non_blocking = false;
                step(non_blocking);
            }
        }
    }

    Region::~Region() {
        stop();

        assert(region_instance == this);
        region_instance = nullptr;
    }

    RegionId Region::id() const {
        return id_;
    }

    auto Region::state() const -> State {
        return state_;
    }

    auto Region::phase() const -> Phase {
        return phase_;
    }

    auto Region::cycle() const -> Cycle {
        return cycle_;
    }

    int Region::file_descriptor() {
        return connection_.client_endpoint().file_descriptor();
    }

    void Region::stop() {
        if (state_ != State::RUNNING) {
            return;
        }

        // Flag that we want to stop and participate until the domain
        // indicates that it is safe to do so.
        state_ = State::STOPPING;
        while (state_ != State::STOPPED) {
            bool non_blocking = false;
            step(non_blocking);
        }
    }

    void Region::step(bool non_blocking) {
        // Start a new cycle if needed. We need to be in the initial phase, and have a reason to do it.
        bool start_cycle = true;
        start_cycle &= phase_ == INITIAL_PHASE;
        start_cycle &= cycle_ == INITIAL_CYCLE || (state_ == State::STOPPING || !ledger_.is_empty());
        if (start_cycle) {
            region_endpoint().send_message(
                Message {
                    .start = {
                        .type = MessageType::START,
                    },
                }
            );

            transition(Phase::RECV_ENTER_SENT_START);
        }

        for (const Message& message: region_endpoint().receive_messages(non_blocking)) {
            debug("[region:{}] received {}", id_, to_string(message.type));
            handle_message(message);
        }

        if (depth_) {
            // `Region::step` and `ObjectFinalizer::finalize` are co-recursive.
            // Short circuiting object finalization in nested `Region::step` calls
            // prevents unbounded stack usage.
            assert(depth_ == 1);
        }
        else {
            ScopedIncrement<size_t> lock(depth_);

            for (size_t i = 0; i < trash_.size(); ++i) {
                try {
                    finalizer_.finalize(*trash_[i]);
                }
                catch (const std::exception& exception) {
                    // The finalizer shouldn't be throwing--it probably indicates a leak.
                    warning("Failed to finalize object:{} - {}", (const void*)trash_[i], exception.what());
                    assert(false);
                }
            }

            trash_.clear();
        }
    }

    void Region::start_increment_operation(Object&, Operation operation) {
        assert(state_ != State::STOPPED);
        assert(operation.type() == OperationType::INCREMENT);

        // JSR: We can avoid accessing the object _at all_ by taking the slow path.
        // Fast-path: This operation can be immediately applied because the object is local.
        // if (id_ == object.region_id()) {
        //     object.apply_increment_operation(operation);
        //     return;
        // }

        // Fast-path: The operation can be added to the current transaction.
        if (LIKELY(ledger_.write(operation))) {
            return;
        }

        flush_operation(operation);
    }

    void Region::start_decrement_operation(Object&, Operation operation) {
        assert(state_ != State::STOPPED);
        assert(operation.type() == OperationType::DECREMENT);

        // Fast-path: The operation can be added to the current transaction.
        if (LIKELY(ledger_.write(operation))) {
            return;
        }

        flush_operation(operation);
    }

    void Region::flush_operation(Operation operation) {
        do {
            bool non_blocking = false;
            step(non_blocking);
        } while (!ledger_.write(operation));
    }

    void Region::handle_abandoned(Object& object) {
        trash_.push_back(&object);
    }

    const OperationLedger& Region::ledger() const {
        return ledger_;
    }

    Endpoint& Region::domain_endpoint() {
        return connection_.server_endpoint();
    }

    Endpoint& Region::region_endpoint() {
        return connection_.client_endpoint();
    }

    void Region::handle_message(const Message& message) {
        assert(state_ != State::STOPPED);

        switch (message.type) {
            case MessageType::START: {
                abort();
            }
            case MessageType::ENTER: {
                assert((phase_ == Phase::RECV_ENTER) || (phase_ == Phase::RECV_ENTER_SENT_START));

                // Wrap up the current transaction and submit ranges of operations
                // that can be applied.
                ledger_.commit_transaction();
                {
                    // Check if the region is ready to stop.
                    bool stop = true;
                    stop &= state_ == State::STOPPING;
                    stop &= ledger_.is_empty();

                    region_endpoint().send_message(
                        Message {
                            .submit = {
                                .type       = MessageType::SUBMIT,
                                .stop       = stop,
                                .increments = ledger_.transaction_log().select(0),
                                .decrements = ledger_.transaction_log().select(2),
                            },
                        }
                    );
                }
                ledger_.begin_transaction();

                transition(message.enter.cycle);
                transition(Phase::RECV_RETIRE);
                break;
            }
            case MessageType::SUBMIT: {
                abort();
            }
            case MessageType::RETIRE: {
                assert(phase_ == Phase::RECV_RETIRE);

                apply_operations<OperationType::INCREMENT>(message.retire.increments);
                apply_operations<OperationType::DECREMENT>(message.retire.decrements);

                transition(Phase::RECV_LEAVE);
                break;
            }
            case MessageType::LEAVE: {
                assert(phase_ == Phase::RECV_LEAVE);

                if (message.leave.stop) {
                    transition(RegionState::STOPPED);
                }

                transition(INITIAL_PHASE);
                break;
            }
        }
    }

    void Region::transition(State next_state) {
        if (state_ == next_state) {
            return;
        }

        debug("[region:{}] transition state {} to {}", id_, to_string(state_), to_string(next_state));
        state_ = next_state;
    }

    void Region::transition(Phase next_phase) {
        if (phase_ == next_phase) {
            return;
        }

        debug("[region:{}] transition phase {} to {}", id_, to_string(phase_), to_string(next_phase));
        phase_ = next_phase;
    }

    void Region::transition(Cycle next_cycle) {
        if (cycle_ == next_cycle) {
            return;
        }

        debug("[region:{}] transition cycle {} to {}", id_, cycle_, next_cycle);
        cycle_ = next_cycle;
    }

    template<OperationType type>
    void Region::apply_operations(OperationRange operations) {
        for (OperationBatch* batch = operations.head; batch != operations.tail; ++batch) {
            for (Operation& operation: batch->operations) {
                if (Object* object = operation.mutable_object()) {
                    object->apply_operation<type>(operation);
                }
                else {
                    // Padding operations don't need to be applied.
                }
            }
        }
    }

    std::string_view to_string(RegionState state) {
        using namespace std::literals;

        switch (state) {
#define X(MANTLE_REGION_STATE)                     \
            case RegionState::MANTLE_REGION_STATE: \
                return #MANTLE_REGION_STATE ##sv;  \

        MANTLE_REGION_STATES(X)
#undef X
        }

        abort();
    }

    std::string_view to_string(RegionPhase phase) {
        using namespace std::literals;

        switch (phase) {
#define X(MANTLE_REGION_PHASE)                     \
            case RegionPhase::MANTLE_REGION_PHASE: \
                return #MANTLE_REGION_PHASE ##sv;  \

        MANTLE_REGION_PHASES(X)
#undef X
        }

        abort();
    }

    bool has_region() {
        return region_instance != nullptr;
    }

    Region& get_region() {
        assert(has_region());

        return *region_instance;
    }

}
