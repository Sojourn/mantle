#pragma once

#include <span>
#include <stdexcept>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <cerrno>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/user.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>

#include "mantle/selector.h"

namespace mantle {

    class PageFaultHandler {
    public:
        enum class Mode {
            MISSING,
            WRITE_PROTECT,
        };

        PageFaultHandler();
        ~PageFaultHandler();

        PageFaultHandler(PageFaultHandler&&) = delete;
        PageFaultHandler(const PageFaultHandler&) = delete;
        PageFaultHandler& operator=(PageFaultHandler&&) = delete;
        PageFaultHandler& operator=(const PageFaultHandler&) = delete;

        int file_descriptor() const;

        template<typename Handler>
        bool poll(Handler&& handler, bool non_blocking);

        void register_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes);
        void unregister_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes);

        void write_protect_memory(std::span<const std::byte> memory);
        void write_unprotect_memory(std::span<const std::byte> memory);

    private:
        static uint64_t translate(const Mode mode);
        static uint64_t translate(const std::initializer_list<Mode> modes);

    private:
        int file_descriptor_;
    };

    template<typename Handler>
    inline bool PageFaultHandler::poll(Handler&& handler, bool non_blocking) {
        struct uffd_msg msg = {};

        if (!non_blocking) {
            wait_for_readable(file_descriptor_);
        }

        ssize_t bytes_read;
        do {
            bytes_read = read(file_descriptor_, &msg, sizeof(msg));
        } while ((bytes_read < 0) && (errno == EINTR));

        if (bytes_read < 0) {
            switch (errno) {
                case EAGAIN:
                    return false;
                default:
                    throw std::runtime_error("Failed to read userfaultfd");
            }
        }

        if (static_cast<size_t>(bytes_read) < sizeof(msg)) {
            throw std::runtime_error("Failed to read userfaultfd (short read)");
        }

        switch (msg.event) {
            case UFFD_EVENT_PAGEFAULT: {
                std::span memory = {
                    reinterpret_cast<std::byte*>(msg.arg.pagefault.address & ~(PAGE_SIZE - 1)),
                    PAGE_SIZE
                };

                if ((msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WRITE) == UFFD_PAGEFAULT_FLAG_WRITE) {
                    handler(memory, Mode::WRITE_PROTECT);
                }
                else {
                    handler(memory, Mode::MISSING);
                }
                break;
            }
            default: {
                // Ignore other events for now. Eventually we'll want to handle virtual memory changes
                // to allow segments to cope with segments being resized.
                break;
            }
        }

        return true;
    }

}
