#include "mantle/page_fault_handler.h"
#include <cassert>

namespace mantle {

    MANTLE_SOURCE_INLINE
    PageFaultHandler::PageFaultHandler()
        : file_descriptor_(-1)
    {
        file_descriptor_ = static_cast<int>(syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK | UFFD_USER_MODE_ONLY));
        if (file_descriptor_ < 0) {
            throw std::runtime_error("Failed to create userfaultfd");
        }

        // API handshake and feature detection must happen before we use the file descriptor.
        {
            constexpr uint64_t required_features = 0;
            constexpr uint64_t optional_features = 0;

            struct uffdio_api uffdio_api;
            memset(&uffdio_api, 0, sizeof(uffdio_api));
            uffdio_api.api = UFFD_API;
            uffdio_api.features = required_features|optional_features;
            uffdio_api.ioctls = _UFFDIO_API | _UFFDIO_REGISTER | _UFFDIO_UNREGISTER;

            if (ioctl(file_descriptor_, UFFDIO_API, &uffdio_api) < 0) {
                throw std::runtime_error("FaultHandler API handshake failed");
            }
            if ((uffdio_api.features & required_features) != required_features) {
                throw std::runtime_error("FaultHandler API missing required features");
            }

            assert(uffdio_api.ioctls & (1ull << _UFFDIO_API));
            assert(uffdio_api.ioctls & (1ull << _UFFDIO_REGISTER));
            assert(uffdio_api.ioctls & (1ull << _UFFDIO_UNREGISTER));
        }
    }

    MANTLE_SOURCE_INLINE
    PageFaultHandler::~PageFaultHandler() {
        const int result = close(file_descriptor_);
        assert(result >= 0);
    }

    MANTLE_SOURCE_INLINE
    int PageFaultHandler::file_descriptor() const {
        return file_descriptor_;
    }

    MANTLE_SOURCE_INLINE
    void PageFaultHandler::register_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes) {
        struct uffdio_register uffdio_register = {};
        uffdio_register.mode = translate(modes);
        uffdio_register.range = {
            .start = reinterpret_cast<uintptr_t>(memory.data()),
            .len = memory.size_bytes(),
        };

        assert((uffdio_register.range.start % PAGE_SIZE) == 0);

        if (ioctl(file_descriptor_, UFFDIO_REGISTER, &uffdio_register) < 0) {
            throw std::runtime_error("Failed to register memory region");
        }
    }

    MANTLE_SOURCE_INLINE
    void PageFaultHandler::unregister_memory(std::span<const std::byte> memory, const std::initializer_list<Mode> modes) {
        struct uffdio_register uffdio_register = {};
        uffdio_register.mode = translate(modes);
        uffdio_register.range = {
            .start = reinterpret_cast<uintptr_t>(memory.data()),
            .len = memory.size_bytes(),
        };

        assert((uffdio_register.range.start % PAGE_SIZE) == 0);

        if (ioctl(file_descriptor_, UFFDIO_UNREGISTER, &uffdio_register) < 0) {
            throw std::runtime_error("Failed to unregister memory region");
        }
    }

    MANTLE_SOURCE_INLINE
    void PageFaultHandler::write_protect_memory(std::span<const std::byte> memory) {
        struct uffdio_writeprotect uffdio_writeprotect = {
            .range = {
                .start = reinterpret_cast<uintptr_t>(memory.data()),
                .len = memory.size_bytes(),
            },
            .mode = UFFDIO_WRITEPROTECT_MODE_WP,
        };

        assert((uffdio_writeprotect.range.start % PAGE_SIZE) == 0);

        if (ioctl(file_descriptor_, UFFDIO_WRITEPROTECT, &uffdio_writeprotect) < 0) {
            throw std::runtime_error("Failed to write protect memory region");
        }
    }

    MANTLE_SOURCE_INLINE
    void PageFaultHandler::write_unprotect_memory(std::span<const std::byte> memory) {
        struct uffdio_writeprotect uffdio_writeprotect = {
            .range = {
                .start = reinterpret_cast<uintptr_t>(memory.data()),
                .len = memory.size_bytes(),
            },
            .mode = 0,
        };

        assert((uffdio_writeprotect.range.start % PAGE_SIZE) == 0);

        if (ioctl(file_descriptor_, UFFDIO_WRITEPROTECT, &uffdio_writeprotect) < 0) {
            throw std::runtime_error("Failed to write unprotect memory region");
        }
    }

    MANTLE_SOURCE_INLINE
    uint64_t PageFaultHandler::translate(const Mode mode) {
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

    MANTLE_SOURCE_INLINE
    uint64_t PageFaultHandler::translate(const std::initializer_list<Mode> modes) {
        uint64_t mask = 0;

        for (const Mode mode : modes) {
            mask |= translate(mode);
        }

        return mask;
    }

}
