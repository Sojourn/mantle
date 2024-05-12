#pragma once

#include <mutex>
#include <thread>
#include <memory>
#include <vector>
#include "mantle/types.h"
#include "mantle/config.h"
#include "mantle/message.h"
#include "mantle/doorbell.h"
#include "mantle/selector.h"
#include "mantle/region.h"
#include "mantle/region_controller.h"

namespace mantle {

    class Region;

    class Domain {
        friend class Region;

    public:
        Domain(const Config& config = Config());
        Domain(Domain&&) = delete;
        Domain(const Domain&) = delete;
        ~Domain();

        Domain& operator=(Domain&&) = delete;
        Domain& operator=(const Domain&) = delete;

        const Config& config() const;

    private:
        void run();

        void handle_event(void* user_data);

        void update_controllers(const RegionControllerCensus& census);
        void start_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);
        void stop_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);

        RegionId bind(Region& region);

    private:
        Config                 config_;
        std::thread            thread_;

        std::mutex             regions_mutex_;
        std::vector<Region*>   regions_;
        RegionControllerGroup  controllers_;

        bool                   running_;
        Doorbell               doorbell_;
        Selector               selector_;
    };

}
