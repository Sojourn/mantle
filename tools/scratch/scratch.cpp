#include <cstdint>
#include <cstddef>
#include <thread>
#include <chrono>
#include <future>
#include <latch>
#include <bitset>
#include "mantle/mantle.h"

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

using namespace mantle;

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
        , temp_page_(MAP_FAILED)
    {
        // Allocate an aligned, temp page.
        {
            temp_page_ = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (temp_page_ == MAP_FAILED) {
                throw std::runtime_error("Failed to allocate a temp page");
            }
        }

        // TODO: Make this non-blocking by default.
        uffd_ = static_cast<int>(syscall(SYS_userfaultfd, O_CLOEXEC | UFFD_USER_MODE_ONLY));
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

    struct Fault {
        std::span<std::byte> memory;
        bool                 is_write = false;
        bool                 is_missing = false;
    };
    // std::span<Fault> poll(bool non_blocking);

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

                switch (msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WRITE) {
                    case 0: {
                        handler(memory, Mode::MISSING);
                        break;
                    }
                    case UFFD_PAGEFAULT_FLAG_WRITE: {
                        handler(memory, Mode::WRITE_PROTECT);
                        break;
                    }
                    default: {
                        assert(false);
                    }
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

    void* temp_page_;
};

class LedgerSegment {
public:
    using Record = std::atomic<void*>;

    explicit LedgerSegment(const size_t initial_size = 16 * 1024) {
        map_size_ = 1ull << log2_ceil(std::max(sizeof(Record) * initial_size, PAGE_SIZE));

        map_addr_ = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (map_addr_ == MAP_FAILED) {
            throw std::runtime_error("Failed to allocate LedgerSegment");
        }

        // Touch memory to ensure backing pages are resident.
        for (Record& record : records()) {
            new(&record) Record{};
        }
    }

    ~LedgerSegment() {
        for (Record& record : records()) {
            record.~Record();
        }

        const int result = munmap(map_addr_, map_size_);
        assert(result >= 0);
    }

    LedgerSegment(LedgerSegment&&) = delete;
    LedgerSegment(const LedgerSegment&) = delete;
    LedgerSegment& operator=(LedgerSegment&&) = delete;
    LedgerSegment& operator=(const LedgerSegment&) = delete;

    [[nodiscard]]
    std::span<std::byte> memory() {
        return {
            static_cast<std::byte*>(map_addr_),
            map_size_ / sizeof(std::byte)
        };
    }

    [[nodiscard]]
    std::span<Record> records() {
        return {
            static_cast<Record*>(map_addr_),
            map_size_ / sizeof(Record)
        };
    }

    [[nodiscard]]
    size_t increment_count() const {
        return increment_count_;
    }

    void set_increment_count(const size_t value) {
        increment_count_ = value;
    }

    [[nodiscard]]
    size_t decrement_count() const {
        return decrement_count_;
    }

    void set_decrement_count(const size_t value) {
        decrement_count_ = value;
    }

    void reset() {
        const size_t count = increment_count_ + decrement_count_;

        for (size_t i = 0; i < count; ++i) {
            records()[i].store(nullptr, std::memory_order_release);
        }

        increment_count_ = 0;
        decrement_count_ = 0;
    }

private:
    void*  map_addr_ = nullptr;
    size_t map_size_ = 0;
    size_t increment_count_ = 0;
    size_t decrement_count_ = 0;
};

class Ledger {
public:
    using Record = std::atomic<void*>; // TODO: Replace with `void*` with `Object*`.
    using Cursor = std::atomic<Record*>;

    Ledger() {
        local_increment_cursor() = segments_[INCREMENT_OFFSET].records().data();
        local_decrement_cursor() = segments_[DECREMENT_OFFSET].records().data();
    }

    LedgerSegment& segment(const Sequence sequence) {
        return segments_[sequence & SEGMENT_MASK];
    }

    void rotate() {
        const size_t old_sequence = sequence_.load(std::memory_order_acquire);
        const size_t new_sequence = old_sequence + 1;

        LedgerSegment& old_inc_segment = segment(old_sequence + INCREMENT_OFFSET);
        LedgerSegment& old_dec_segment = segment(old_sequence + DECREMENT_OFFSET);
        LedgerSegment& new_inc_segment = segment(new_sequence + INCREMENT_OFFSET);
        LedgerSegment& new_dec_segment = segment(new_sequence + DECREMENT_OFFSET);

        Cursor& increment_cursor = local_increment_cursor();
        Cursor& decrement_cursor = local_decrement_cursor();

        increment_counts_[(old_sequence + INCREMENT_OFFSET) & SEGMENT_MASK] = (increment_cursor.load(std::memory_order_acquire) - old_inc_segment.records().data()) - decrement_counts_[old_sequence & SEGMENT_MASK];
        decrement_counts_[(old_sequence + DECREMENT_OFFSET) & SEGMENT_MASK] = (decrement_cursor.load(std::memory_order_acquire) - old_dec_segment.records().data());

        increment_cursor.store(new_inc_segment.records().data() + decrement_counts_[new_sequence & SEGMENT_MASK], std::memory_order_release);
        decrement_cursor.store(new_dec_segment.records().data(), std::memory_order_release);

        sequence_.store(new_sequence, std::memory_order_release);
    }

    static Cursor& local_increment_cursor() {
        thread_local Cursor cursor = nullptr;
        return cursor;
    }

    static std::atomic<std::atomic<void*>*>& local_decrement_cursor() {
        thread_local Cursor cursor = nullptr;
        return cursor;
    }

    MANTLE_HOT
    static void write_increment(void* value) {
        Cursor& cursor = local_increment_cursor();
        Record* record = cursor.load(std::memory_order_acquire);

        record->store(value, std::memory_order_release);
        cursor.store(record + 1, std::memory_order_release);
    }

    MANTLE_HOT
    static void write_decrement(void* value) {
        Cursor& cursor = local_decrement_cursor();
        Record* record = cursor.load(std::memory_order_acquire);

        record->store(value, std::memory_order_release);
        cursor.store(record + 1, std::memory_order_release);
    }

private:
    static constexpr size_t SEGMENT_COUNT = 4;
    static constexpr size_t SEGMENT_MASK = SEGMENT_COUNT - 1;
    static constexpr size_t INCREMENT_OFFSET = 0;
    static constexpr size_t DECREMENT_OFFSET = 2;

    std::atomic_size_t                       sequence_;
    std::array<LedgerSegment, SEGMENT_COUNT> segments_;
    std::array<size_t, SEGMENT_COUNT>        increment_counts_;
    std::array<size_t, SEGMENT_COUNT>        decrement_counts_;
};

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    LedgerSegment segment;

    PageFaultHandler page_fault_handler;
    page_fault_handler.register_memory(segment.memory(), {PageFaultHandler::Mode::MISSING, PageFaultHandler::Mode::WRITE_PROTECT});
    page_fault_handler.write_protect_memory(segment.memory().last(PAGE_SIZE));

    std::thread fault_thread([&] {
        uintptr_t foo = 0;
        void* foo_ptr = &foo;

        for (std::atomic<void*>& record: segment.records()) {
            record.store(foo_ptr, std::memory_order_release);
        }
    });

    page_fault_handler.poll([&](std::span<std::byte> memory, PageFaultHandler::Mode) {
        std::cout << "Handling fault" << std::endl;

        size_t count = 0;
        for (std::atomic<void*>& record: segment.records()) {
            if (const void* value = record.load(std::memory_order_acquire); value) {
                count += 1;
            }
            else {
                break;
            }
        }
        std::cout << count << " writes detected out of " << segment.records().size() << " total" << std::endl;
        page_fault_handler.write_unprotect_memory(memory);
    });

    fault_thread.join();

    return EXIT_SUCCESS;
}
