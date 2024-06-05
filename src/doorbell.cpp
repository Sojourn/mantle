#include "mantle/doorbell.h"
#include "mantle/selector.h"
#include "mantle/config.h"
#include <stdexcept>
#include <cstring>
#include <fmt/core.h>
#include <unistd.h>
#include <sys/eventfd.h>

namespace mantle {

    MANTLE_SOURCE_INLINE
    Doorbell::Doorbell()
        : file_descriptor_(eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK))
    {
        if (file_descriptor_ < 0) {
            throw std::runtime_error(fmt::format("Failed to create doorbell eventfd - {}", strerror(errno)));
        }
    }

    MANTLE_SOURCE_INLINE
    Doorbell::~Doorbell() {
        close(file_descriptor_);
    }

    MANTLE_SOURCE_INLINE
    int Doorbell::file_descriptor() {
        return file_descriptor_;
    }

    MANTLE_SOURCE_INLINE
    void Doorbell::ring(uint64_t count) {
        ssize_t bytes_written = 0;
        do {
            bytes_written = write(file_descriptor_, &count, sizeof(count));
        } while ((bytes_written < 0) && (errno == EINTR));

        if (bytes_written != static_cast<ssize_t>(sizeof(count))) {
            abort();
        }
    }

    MANTLE_SOURCE_INLINE
    uint64_t Doorbell::poll(bool non_blocking) {
        if (!non_blocking) {
            wait_for_readable(file_descriptor_);
        }

        uint64_t count = 0;
        ssize_t bytes_read = 0;
        do {
            bytes_read = read(file_descriptor_, &count, sizeof(count));
        } while ((bytes_read < 0) && (errno == EINTR));

        if (bytes_read != static_cast<ssize_t>(sizeof(count))) {
            if (bytes_read < 0) {
                if (errno == EAGAIN) {
                    return 0;
                }
            }

            abort();
        }

        return count;
    }

}
