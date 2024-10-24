#include "mantle/region.h"
#include "mantle/domain.h"
#include "mantle/object.h"
#include "mantle/finalizer.h"
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

    MANTLE_SOURCE_INLINE
    Region::Region(Domain& domain, Finalizer& finalizer)
        : domain_(domain)
        , id_(std::numeric_limits<RegionId>::max())
        , state_(INITIAL_STATE)
        , phase_(INITIAL_PHASE)
        , cycle_(INITIAL_CYCLE)
        , depth_(0)
        , finalizer_(finalizer)
        , ledger_(domain.write_barrier_manager_)
    {
        // Register ourselves as the region on this thread.
        {
            Region*& instance = thread_local_instance();
            if (instance) {
                throw std::runtime_error("Cannot have more than one region per thread");
            }

            instance = this;
        }

        // Synchronize with other regions until our cycle and phase match.
        {
            id_ = domain_.bind(*this);
            while (cycle_ == INITIAL_CYCLE) {
                constexpr bool non_blocking = false;
                step(non_blocking);
            }
        }
    }

    MANTLE_SOURCE_INLINE
    Region::~Region() {
        stop();

        thread_local_instance() = nullptr;
    }

    MANTLE_SOURCE_INLINE
    RegionId Region::id() const {
        return id_;
    }

    MANTLE_SOURCE_INLINE
    auto Region::state() const -> State {
        return state_;
    }

    MANTLE_SOURCE_INLINE
    auto Region::phase() const -> Phase {
        return phase_;
    }

    MANTLE_SOURCE_INLINE
    auto Region::cycle() const -> Cycle {
        return cycle_;
    }

    MANTLE_SOURCE_INLINE
    int Region::file_descriptor() {
        return connection_.client_endpoint().file_descriptor();
    }

    MANTLE_SOURCE_INLINE
    void Region::stop() {
        if (state_ != State::RUNNING) {
            return;
        }

        if (domain_.parent_thread_id_ == get_tid()) {
            domain_.stop(); // Prevent a region and domain on the same thread from live-locking.
        }

        // Flag that we want to stop and participate until the domain
        // indicates that it is safe to do so.
        state_ = State::STOPPING;
        while (state_ != State::STOPPED) {
            constexpr bool non_blocking = false;
            step(non_blocking);
        }
    }

    MANTLE_SOURCE_INLINE
    void Region::step(const bool non_blocking) {
        if (depth_) {
            // Guard against the finalizer calling `Region::step`.
            assert(false);
            return;
        }

        ScopedIncrement lock(depth_);

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

        finalize_garbage();
    }

    MANTLE_SOURCE_INLINE
    Endpoint& Region::domain_endpoint() {
        return connection_.server_endpoint();
    }

    MANTLE_SOURCE_INLINE
    Endpoint& Region::region_endpoint() {
        return connection_.client_endpoint();
    }

    MANTLE_SOURCE_INLINE
    void Region::handle_message(const Message& message) {
        assert(state_ != State::STOPPED);

        switch (message.type) {
            case MessageType::START: {
                abort();
            }
            case MessageType::ENTER: {
                assert((phase_ == Phase::RECV_ENTER) || (phase_ == Phase::RECV_ENTER_SENT_START));

                ledger_.commit();
                {
                    // Check if the region is ready to stop.
                    bool stop = true;
                    stop &= state_ == State::STOPPING;
                    stop &= ledger_.is_empty();

                    region_endpoint().send_message(
                        Message {
                            .submit = {
                                .type          = MessageType::SUBMIT,
                                .stop          = stop,
                                .write_barrier = &ledger_.barrier(WriteBarrierPhase::APPLY),
                            },
                        }
                    );
                }

                transition(message.enter.cycle);
                transition(Phase::RECV_RETIRE);
                break;
            }
            case MessageType::SUBMIT: {
                abort();
            }
            case MessageType::RETIRE: {
                assert(phase_ == Phase::RECV_RETIRE);

                assert(!garbage_);
                garbage_ = message.retire.garbage;

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

    MANTLE_SOURCE_INLINE
    void Region::transition(const State next_state) {
        if (state_ == next_state) {
            return;
        }

        debug("[region:{}] transition state {} to {}", id_, to_string(state_), to_string(next_state));
        state_ = next_state;
    }

    MANTLE_SOURCE_INLINE
    void Region::transition(const Phase next_phase) {
        if (phase_ == next_phase) {
            return;
        }

        debug("[region:{}] transition phase {} to {}", id_, to_string(phase_), to_string(next_phase));
        phase_ = next_phase;
    }

    MANTLE_SOURCE_INLINE
    void Region::transition(const Cycle next_cycle) {
        if (cycle_ == next_cycle) {
            return;
        }

        debug("[region:{}] transition cycle {} to {}", id_, cycle_, next_cycle);
        cycle_ = next_cycle;
    }

    MANTLE_SOURCE_INLINE
    void Region::finalize_garbage() {
        if (!garbage_) {
            return;
        }

        if constexpr (MANTLE_ENABLE_OBJECT_GROUPING) {
            assert(garbage_->object_count == garbage_->group_offsets[garbage_->group_max + 1]);

            garbage_->for_each_group([this](ObjectGroup group, std::span<Object*> members) {
                finalizer_.finalize(group, members);
            });
        }
        else {
            for (size_t i = 0; i < garbage_->object_count; ++i) {
                Object* object = garbage_->objects[i];
                finalizer_.finalize(object->group(), std::span{&object, 1});
            }
        }

        garbage_.reset();
    }

    MANTLE_SOURCE_INLINE
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

    MANTLE_SOURCE_INLINE
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

}
