#pragma once

#include <cstdint>
#include <cstddef>

#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/user.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/userfaultfd.h>

namespace mantle {

    class PageFaultHandler {
    public:
        enum class Mode {
            MISSING,
            WRITE_PROTECT,
        };

        PageFaultHandler()
            : uffd_(-1)
            , has_feature_thread_id_(false)
            , has_feature_exact_address_(false)
        {
            uffd_ = static_cast<int>(syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK | UFFD_USER_MODE_ONLY));
            if (uffd_ < 0) {
                throw std::runtime_error("Failed to create userfaultfd");
            }

            // API handshake and feature detection must happen before we use the file descriptor.
            {
                constexpr uint64_t required_features = 0;
                constexpr uint64_t optional_features = UFFD_FEATURE_THREAD_ID|UFFD_FEATURE_EXACT_ADDRESS;

                struct uffdio_api uffdio_api;
                memset(&uffdio_api, 0, sizeof(uffdio_api));
                uffdio_api.api = UFFD_API;
                uffdio_api.features = required_features|optional_features;
                uffdio_api.ioctls = _UFFDIO_API | _UFFDIO_REGISTER | _UFFDIO_UNREGISTER;

                if (ioctl(uffd_, UFFDIO_API, &uffdio_api) < 0) {
                    throw std::runtime_error("FaultHandler API handshake failed");
                }
                if ((uffdio_api.features & required_features) != required_features) {
                    throw std::runtime_error("FaultHandler API missing required features");
                }

                has_feature_thread_id_ = static_cast<bool>(uffdio_api.features & UFFD_FEATURE_THREAD_ID);
                has_feature_exact_address_ = static_cast<bool>(uffdio_api.features & UFFD_FEATURE_EXACT_ADDRESS);

                assert(uffdio_api.ioctls & (1ull << _UFFDIO_API));
                assert(uffdio_api.ioctls & (1ull << _UFFDIO_REGISTER));
                assert(uffdio_api.ioctls & (1ull << _UFFDIO_UNREGISTER));
            }
        }

        ~PageFaultHandler() {
            const int result = close(uffd_);
            assert(result >= 0);
        }

        PageFaultHandler(PageFaultHandler&&) = delete;
        PageFaultHandler(const PageFaultHandler&) = delete;
        PageFaultHandler& operator=(PageFaultHandler&&) = delete;
        PageFaultHandler& operator=(const PageFaultHandler&) = delete;

        int file_descriptor() const {
            return uffd_;
        }

        // TODO: Iterate on this API. It's a little rough right now.
        template<typename Handler>
        bool poll(Handler&& handler) {
            struct uffd_msg msg = {};

            ssize_t bytes_read;
            do {
                bytes_read = read(uffd_, &msg, sizeof(msg));
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

        void register_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes) {
            struct uffdio_register uffdio_register = {};
            uffdio_register.mode = translate(modes);
            uffdio_register.range = {
                .start = reinterpret_cast<uintptr_t>(memory.data()),
                .len = memory.size_bytes(),
            };

            assert((uffdio_register.range.start % PAGE_SIZE) == 0);

            if (ioctl(uffd_, UFFDIO_REGISTER, &uffdio_register) < 0) {
                throw std::runtime_error("Failed to register memory region");
            }
        }

        void unregister_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes) {
            struct uffdio_register uffdio_register = {};
            uffdio_register.mode = translate(modes);
            uffdio_register.range = {
                .start = reinterpret_cast<uintptr_t>(memory.data()),
                .len = memory.size_bytes(),
            };

            assert((uffdio_register.range.start % PAGE_SIZE) == 0);

            if (ioctl(uffd_, UFFDIO_UNREGISTER, &uffdio_register) < 0) {
                throw std::runtime_error("Failed to unregister memory region");
            }
        }

        void write_protect_memory(std::span<const std::byte> memory) {
            struct uffdio_writeprotect uffdio_writeprotect = {
                .range = {
                    .start = reinterpret_cast<uintptr_t>(memory.data()),
                    .len = memory.size_bytes(),
                },
                .mode = UFFDIO_WRITEPROTECT_MODE_WP,
            };

            assert((uffdio_writeprotect.range.start % PAGE_SIZE) == 0);

            if (ioctl(uffd_, UFFDIO_WRITEPROTECT, &uffdio_writeprotect) < 0) {
                throw std::runtime_error("Failed to write protect memory region");
            }
        }

        void write_unprotect_memory(std::span<const std::byte> memory) {
            struct uffdio_writeprotect uffdio_writeprotect = {
                .range = {
                    .start = reinterpret_cast<uintptr_t>(memory.data()),
                    .len = memory.size_bytes(),
                },
                .mode = 0,
            };

            assert((uffdio_writeprotect.range.start % PAGE_SIZE) == 0);

            if (ioctl(uffd_, UFFDIO_WRITEPROTECT, &uffdio_writeprotect) < 0) {
                throw std::runtime_error("Failed to write unprotect memory region");
            }
        }

    private:
        static uint64_t translate(const Mode mode) {
            switch (mode) {
                case Mode::MISSING: {
                    return UFFDIO_REGISTER_MODE_MISSING;
                }
                case Mode::WRITE_PROTECT: {
                    return UFFDIO_REGISTER_MODE_WP;
                }
            }

            __builtin_unreachable();
        }

        static uint64_t translate(const std::initializer_list<Mode> modes) {
            uint64_t mask = 0;

            for (const Mode mode : modes) {
                mask |= translate(mode);
            }

            return mask;
        }

    private:
        int  uffd_;
        bool has_feature_thread_id_;
        bool has_feature_exact_address_;
    };

}
