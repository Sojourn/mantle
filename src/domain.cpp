#include "mantle/domain.h"
#include "mantle/util.h"
#include "mantle/debug.h"
#include <future>
#include <cstdlib>
#include <cassert>

namespace mantle {

    MANTLE_SOURCE_INLINE
    Domain::Domain(std::optional<std::span<size_t>> thread_cpu_affinities)
        : running_(false)
    {
        selector_.add_watch(doorbell_.file_descriptor(), &doorbell_);
        selector_.add_watch(write_barrier_manager_.file_descriptor(), &write_barrier_manager_);

        std::promise<void> init_promise;
        std::future<void> init_future = init_promise.get_future();

        thread_ = std::thread([init_promise = std::move(init_promise), thread_cpu_affinities, this]() mutable {
            try {
                debug("[domain] initializing thread");
                if (thread_cpu_affinities) {
                    set_cpu_affinity(*thread_cpu_affinities);
                }

                init_promise.set_value();
            }
            catch (...) {
                init_promise.set_exception(std::current_exception());
                return;
            }

            debug("[domain] starting");
            run();
            debug("[domain] stopping");
        });

        init_future.get();
    }

    MANTLE_SOURCE_INLINE
    Domain::~Domain() {
        thread_.join();
    }

    MANTLE_SOURCE_INLINE
    WriteBarrierManager& Domain::write_barrier_manager() {
        return write_barrier_manager_;
    }

    MANTLE_SOURCE_INLINE
    void Domain::run() {
        running_ = true;

        while (running_) {
            constexpr bool non_blocking = false;
            for (void* user_data: selector_.poll(non_blocking)) {
                handle_event(user_data);
            }

            // Alternate between checking if controllers need to transmit and 
            // updating controller state until quiescent.
            RegionControllerCensus census(controllers_);
            while (true) {
                update_controllers(census);

                for (RegionId region_id = 0; size_t{region_id} < controllers_.size(); ++region_id) {
                    Region& region = *regions_[region_id];
                    RegionController& controller = *controllers_[region_id];

                    while (std::optional<Message> message = controller.send_message()) {
                        if (region.domain_endpoint().send_message(*message)) {
                            debug("[region_controller:{}] sent {}", region_id, to_string(message->type));
                        }
                        else {
                            abort();
                        }
                    }
                }

                // Update the census and break if nothing changed.
                if (std::exchange(census, RegionControllerCensus(controllers_)) == census) {
                    break;
                }
            }
        }
    }

    MANTLE_SOURCE_INLINE
    void Domain::handle_event(void* user_data) {
        constexpr bool non_blocking = true;

        if (user_data == &write_barrier_manager_) {
            // Resolve a write protection fault and resume the region.
            write_barrier_manager_.poll(non_blocking);
        }
        else if (user_data == &doorbell_) {
            // We'll add a controller for the new region later.
            // Re-arm the doorbell now that we've awoken.
            doorbell_.poll(non_blocking);
        }
        else {
            Region& region = *static_cast<Region*>(user_data);
            RegionController& controller = *controllers_[region.id()];
            for (const Message& message: region.domain_endpoint().receive_messages(non_blocking)) {
                debug("[region_controller:{}] received {}", region.id(), to_string(message.type));
                controller.receive_message(message);
            }
        }
    }

    MANTLE_SOURCE_INLINE
    void Domain::update_controllers(const RegionControllerCensus& census) {
        // Check if there are controllers that need to be started or stopped.
        // This is safe to do while there isn't an active cycle.
        if (controllers_.empty() || census.any(RegionControllerPhase::START)) {
            std::scoped_lock lock(mutex_);

            if (controllers_.size() < regions_.size()) {
                start_controllers(census, lock);
            }
            else if (census.all(RegionControllerState::STOPPING)) {
                stop_controllers(census, lock);
            }
            else if (census.all(RegionControllerState::SHUTDOWN)) {
                running_ = false;
            }
        }

        // Synchronize at barrier phases.
        for (auto&& controller: controllers_) {
            controller->synchronize(census);
        }
    }

    MANTLE_SOURCE_INLINE
    void Domain::start_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&) {
        for (RegionId region_id = controllers_.size(); region_id < regions_.size(); ++region_id) {
            Region& region = *regions_[region_id];

            // Create a controller to manage the region.
            {
                auto controller = std::make_unique<RegionController>(
                    region_id,
                    controllers_,
                    write_barrier_manager_
                );

                controller->start(census.max_cycle());
                controllers_.push_back(std::move(controller));
            }

            // Monitor the connection associated with this region so we can wake up
            // when it is readable and check for messages.
            selector_.add_watch(region.domain_endpoint().file_descriptor(), &region);
        }
    }

    MANTLE_SOURCE_INLINE
    void Domain::stop_controllers(const RegionControllerCensus&, std::scoped_lock<std::mutex>&) {
        bool is_quiescent = true;
        for (auto&& controller: controllers_) {
            is_quiescent &= controller->is_quiescent();
        }

        if (is_quiescent) {
            for (auto&& controller: controllers_) {
                controller->stop();
            }
        }
        else {
            // One or more controllers are still flushing operations.
        }
    }

    MANTLE_SOURCE_INLINE
    RegionId Domain::bind(Region& region) {
        std::scoped_lock lock(mutex_);

        const RegionId region_id = regions_.size();
        regions_.push_back(&region);
        doorbell_.ring();

        return region_id;
    }

}
