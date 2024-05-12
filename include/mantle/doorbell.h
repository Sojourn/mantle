#pragma once

#include <cstdint>
#include <cstddef>

namespace mantle {

    class Doorbell {
        Doorbell(Doorbell&&) = delete;
        Doorbell(const Doorbell&) = delete;
        Doorbell& operator=(Doorbell&&) = delete;
        Doorbell& operator=(const Doorbell&) = delete;

    public:
        Doorbell();
        ~Doorbell();

        // Returns file descriptor that will indicate when the doorbell is ringing.
        int file_descriptor();

        // Ring the doorbell a number of times.
        void ring(uint64_t count = 1);

        // Return the number of times the doorbell has been rung since last polled.
        uint64_t poll(bool non_blocking);

    private:
        int file_descriptor_;
    };

}
