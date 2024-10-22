#pragma once

#include <optional>
#include <vector>
#include <thread>
#include <mutex>
#include "mantle/types.h"
#include "mantle/config.h"
#include "mantle/doorbell.h"
#include "mantle/selector.h"
#include "mantle/region.h"
#include "mantle/ledger.h"
#include "mantle/region_controller.h"

namespace mantle {

    class Region;

    enum class DomainState {
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
    };

    class Domain {
        friend class Region;

    public:
        explicit Domain(std::optional<std::span<size_t>> thread_cpu_affinities = std::nullopt);
        ~Domain();

        Domain(Domain&&) = delete;
        Domain(const Domain&) = delete;
        Domain& operator=(Domain&&) = delete;
        Domain& operator=(const Domain&) = delete;

        DomainState state() const;

        void stop();

    private:
        void run();

        void handle_event(void* user_data);

        void update_controllers(const RegionControllerCensus& census);
        void start_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);
        void stop_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);

        RegionId bind(Region& region);

    private:
        // These members should only be accessed while the mutex is held.
        struct Shared {
            mutable std::mutex   mutex;
            DomainState          state = DomainState::STARTING;
            std::vector<Region*> regions;
        };

        std::thread            thread_;
        Shared                 shared_;

        pid_t                  parent_thread_id_;
        WriteBarrierManager    write_barrier_manager_;

        RegionControllerGroup  controllers_;
        std::vector<Endpoint*> endpoints_;
        Doorbell               doorbell_;
        Selector               selector_;
    };

}
