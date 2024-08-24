#pragma once

#define MANTLE_SINGLE_HEADER

#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <climits>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <iostream>
#include <limits>
#include <linux/userfaultfd.h>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <ostream>
#include <poll.h>
#include <sched.h>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

// include/mantle/config.h

// This trades off memory usage for a reduced number of write protection faults
// that need to be handled to extend write barriers.
// Ideally this is sized such that write protection faults never happen in steady state.
#ifndef MANTLE_WRITE_BARRIER_SEGMENT_CAPACITY
#  define MANTLE_WRITE_BARRIER_SEGMENT_CAPACITY (16 * 1024)
#endif

// Attempt to merge reference count updates acting on the same object.
// This can help reduce the number of random memory writes when applying updates.
#ifndef MANTLE_ENABLE_OBJECT_GROUPING
#  define MANTLE_ENABLE_OBJECT_GROUPING true
#endif

// TODO: Do platform detection, this is lazy.
#ifndef MANTLE_CACHE_LINE_SIZE
#  define MANTLE_CACHE_LINE_SIZE 64
#endif

// The number of messages that can be queued between `Domain` and `Region` endpoints.
// TODO: This will never change, and can be hard-coded.
#ifndef MANTLE_STREAM_CAPACITY
#  define MANTLE_STREAM_CAPACITY 4096
#endif


// include/mantle/types.h


namespace mantle {

    class Object;

    using RegionId        = uint16_t;
    using ObjectGroup     = uint16_t;
    using ObjectGroupMask = std::array<uint64_t, std::numeric_limits<ObjectGroup>::max() / (sizeof(uint64_t) * CHAR_BIT)>;
    using AtomicSequence  = std::atomic_uint64_t;
    using Sequence        = AtomicSequence::value_type;

    // TODO: Move this into a separate file. It knows too much aabout other classes.
    struct ObjectGroups {
        Object**         objects;
        size_t           object_count;
        ObjectGroup      group_min;     // Inclusive. TODO: rename to `min_group`.
        ObjectGroup      group_max;     // Inclusive. TODO: rename to `max_group`.
        size_t*          group_offsets; // Offsets into the objects array (where to find members).
        ObjectGroupMask* group_mask;    // A bitset of non-empty groups.

        [[nodiscard]]
        size_t group_member_count(ObjectGroup group) const {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            return group_offsets[static_cast<size_t>(group) + 1] - group_offsets[group];
        }

        [[nodiscard]]
        std::span<Object*> group_members(ObjectGroup group) {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            const size_t offset = group_offsets[static_cast<size_t>(group)];
            const size_t length = group_member_count(group);

            return {
                &objects[offset],
                length
            };
        }

        template<typename Visitor>
        void for_each_group(Visitor&& visitor) {
            if constexpr (!MANTLE_ENABLE_OBJECT_GROUPING) {
                abort();
            }

            for (ObjectGroup group = group_min; group <= group_max; ++group) {
                if (std::span<Object*> members = group_members(group); !members.empty()) {
                    visitor(group, members);
                }
            }
        }
    };

    static constexpr RegionId INVALID_REGION_ID = std::numeric_limits<RegionId>::max();

}


// include/mantle/util.h


#ifdef __GNUC__
#  define MANTLE_HOT __attribute__((hot, always_inline)) inline
#  ifdef MANTLE_SINGLE_HEADER
#    define MANTLE_COLD
#  else
#    define MANTLE_COLD __attribute__((noinline))
#  endif
#else
#  define MANTLE_HOT inline
#  define MANTLE_COLD inline
#endif

#ifndef LIKELY 
#  define LIKELY(x)   __builtin_expect(!!(x), 1)
#endif

#ifndef UNLIKELY
#  define UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

namespace mantle {

    template<typename T>
    constexpr bool is_power_of_2(T value) {
        return value && (!(value & (value - 1)));
    }

    template<typename T>
    constexpr T log2_floor(T value) {
        if (value == 1) {
            return 0;
        }

        return log2_floor(value >> 1) + 1;
    }

    template<typename T>
    constexpr T log2_ceil(T value) {
        if (value == 1) {
            return 0;
        }

        return log2_floor(value - 1) + 1;
    }

    inline void set_cpu_affinity(std::span<size_t> cpus) {
        cpu_set_t set;
        CPU_ZERO(&set);

        for (const size_t cpu: cpus) {
            CPU_SET(static_cast<int>(cpu), &set);
        }

        if (sched_setaffinity(0, sizeof(set), &set) < 0) {
            throw std::runtime_error("Failed to set cpu affinity");
        }

        std::this_thread::yield();
    }

    inline pid_t get_tid() {
        return syscall(SYS_gettid);
    }

}


// include/mantle/selector.h


namespace mantle {

    class Selector {
        Selector(Selector&&);
        Selector(const Selector&);
        Selector& operator=(Selector&&);
        Selector& operator=(const Selector&);

    public:
        Selector();
        ~Selector();

        // Returns an array of user-data corresponding to file descriptors that are ready-to-read.
        std::span<void*> poll(bool non_blocking);

        void add_watch(int file_descriptor, void* user_data);
        void modify_watch(int file_descriptor, void* user_data);
        void delete_watch(int file_descriptor);

    private:
        static constexpr size_t MAX_EVENT_COUNT = 16;

        int                                epoll_fd_;
        std::array<void*, MAX_EVENT_COUNT> poll_results_;
    };

    void wait_for_readable(int file_descriptor);

}


// include/mantle/operation.h


namespace mantle {

    class Object;

    enum class OperationType : uint8_t {
        INCREMENT,
        DECREMENT,
    };

    constexpr size_t OPERATION_TYPE_COUNT = 2;

    struct Operation {
        // Lower 3 bit of the tag encode an exponent which is used for greater range.
        static constexpr uintptr_t EXPONENT_BITS  = 2;
        static constexpr uintptr_t EXPONENT_SHIFT = 0;
        static constexpr uintptr_t EXPONENT_MASK  = ((1ull << EXPONENT_BITS) - 1) << EXPONENT_SHIFT;
        static constexpr uintptr_t EXPONENT_MIN   = 0;
        static constexpr uintptr_t EXPONENT_MAX   = EXPONENT_MASK;

        // Upper bit of the tag encodes the type (sign).
        static constexpr uintptr_t TYPE_BITS  = 1;
        static constexpr uintptr_t TYPE_SHIFT = EXPONENT_SHIFT + EXPONENT_BITS;
        static constexpr uintptr_t TYPE_MASK  = ((1ull << TYPE_BITS) - 1) << TYPE_SHIFT;

        static constexpr uintptr_t TAG_BITS = EXPONENT_BITS + TYPE_BITS;
        static constexpr uintptr_t TAG_MASK = (1ull << TAG_BITS) - 1;

        static constexpr uintptr_t POINTER_BITS = (sizeof(Object*) * CHAR_BIT) - TAG_BITS;
        static constexpr uintptr_t POINTER_MASK = ~TAG_MASK;

        auto operator<=>(const Operation&) const noexcept = default;

        [[nodiscard]]
        explicit operator bool() const noexcept {
            return tagged_pointer_ != 0;
        }

        [[nodiscard]]
        const Object* object() const noexcept {
            const uintptr_t pointer = tagged_pointer_ & POINTER_MASK;

            Object* object;
            memcpy(&object, &pointer, sizeof(object));
            return std::launder(object); // Implicit conversion to `const Object*`.
        }

        [[nodiscard]]
        Object* mutable_object() const noexcept {
            const uintptr_t pointer = tagged_pointer_ & POINTER_MASK;

            Object* object;
            memcpy(&object, &pointer, sizeof(object));
            return std::launder(object);
        }

        [[nodiscard]]
        OperationType type() const noexcept {
            return static_cast<OperationType>((tagged_pointer_ & TYPE_MASK) >> TYPE_SHIFT);
        }

        [[nodiscard]]
        uint8_t exponent() const noexcept {
            return static_cast<uint8_t>((tagged_pointer_ & EXPONENT_MASK) >> EXPONENT_SHIFT);
        }

        [[nodiscard]]
        uint8_t magnitude() const noexcept {
            return 1u << exponent();
        }

        [[nodiscard]]
        int64_t value() const noexcept {
            switch (type()) {
                case OperationType::INCREMENT: {
                    return +1 << exponent();
                }
                case OperationType::DECREMENT: {
                    return -1 << exponent();
                }
            }

            abort(); // Unreachable.
        }

        uintptr_t tagged_pointer_;
    };
    static_assert(std::is_trivial_v<Operation>, "Operation must be a trivial type.");

    inline Operation make_operation(Object* object, const OperationType type, const uint8_t exponent = 0) {
        assert(exponent <= Operation::EXPONENT_MAX);

        uintptr_t pointer = 0;
        memcpy(&pointer, &object, sizeof(pointer));

        uintptr_t tag = 0;
        tag |= (static_cast<uint8_t>(type) << Operation::TYPE_SHIFT);
        tag |= (static_cast<uint8_t>(exponent) << Operation::EXPONENT_SHIFT);

        return {
            .tagged_pointer_ = pointer | tag,
        };
    }

    inline Operation make_null_operation() {
        return make_operation(nullptr, OperationType::INCREMENT);
    }

    inline Operation make_increment_operation(Object* object, const uint8_t exponent = 0) {
        return make_operation(object, OperationType::INCREMENT, exponent);
    }

    inline Operation make_decrement_operation(Object* object, const uint8_t exponent = 0) {
        return make_operation(object, OperationType::DECREMENT, exponent);
    }

    constexpr size_t to_index(OperationType type) {
        return static_cast<size_t>(type);
    }

    constexpr std::string_view to_string(const OperationType type) {
        using namespace std::literals;

        switch (type) {
            case OperationType::INCREMENT: return "INCREMENT"sv;
            case OperationType::DECREMENT: return "DECREMENT"sv;
        }

        abort();
    }

}


// include/mantle/page_fault_handler.h




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


// include/mantle/object.h


namespace mantle {

    // This alignment gives us 3 tag bits to use in the encoding of an operation.
    class alignas(8) Object {
    public:
        explicit Object(ObjectGroup group = 0);
        ~Object();

        Object(Object&&) = delete;
        Object(const Object&) = delete;
        Object& operator=(Object&&) = delete;
        Object& operator=(const Object&) = delete;

        [[nodiscard]]
        bool is_managed() const;

        [[nodiscard]]
        RegionId region_id() const;

        [[nodiscard]]
        ObjectGroup group() const;

    private:
        template<typename T>
        friend class Ref;
        template<typename T>
        friend class Ptr;

        friend class RegionController;

        // Associate this `Object` to the local `Region`. Reference counting
        // and object finalization will be handled by that `Region. An `Object`
        // can only be bound once, when a handle to it is first created.
        //
        void bind(RegionId region_id);

        // Update the reference count of this `Object` by the given magnitude.
        // These functions return `true` if the reference count remains positive.
        bool apply_increment(uint32_t delta_magnitude);
        bool apply_decrement(uint32_t delta_magnitude);

    private:
        uint32_t    reference_count_;
        RegionId    region_id_;
        ObjectGroup group_;
    };

    // Ensure that we can pack a tag and pointer into an Operation.
    static_assert(alignof(Object) >= (1ull << Operation::TAG_BITS));
    static_assert(sizeof(Object*) == sizeof(Operation));

}


// include/mantle/ledger.h



#define WRITE_BARRIER_PHASES(X) \
    X(STORE_DECREMENTS)         \
    X(DELAY)                    \
    X(STORE_INCREMENTS)         \
    X(APPLY)                    \

namespace mantle {

    class Ledger;
    class WriteBarrier;

    enum class WriteBarrierPhase {
#define X(WRITE_BARRIER_PHASE) WRITE_BARRIER_PHASE,
        WRITE_BARRIER_PHASES(X)
#undef X
    };

    constexpr size_t WRITE_BARRIER_PHASE_COUNT = 0
#define X(WRITE_BARRIER_PHASE) + 1
        WRITE_BARRIER_PHASES(X)
#undef X
    ;

    // This is a simple RAII wrapper around a private anonymous memory mapping.
    class PrivateMemoryMapping {
    public:
        explicit PrivateMemoryMapping(size_t size, bool populate = true);
        ~PrivateMemoryMapping();

        PrivateMemoryMapping(PrivateMemoryMapping&&) = delete;
        PrivateMemoryMapping(const PrivateMemoryMapping&) = delete;
        PrivateMemoryMapping& operator=(PrivateMemoryMapping&&) = delete;
        PrivateMemoryMapping& operator=(const PrivateMemoryMapping&) = delete;

        [[nodiscard]]
        std::span<std::byte> memory();

        [[nodiscard]]
        std::span<const std::byte> memory() const;

    private:
        std::span<std::byte> memory_;
    };

    // An object pointer vector. It is divided into increment and decrement sections
    // which are written in their respective phases.
    struct WriteBarrierSegment {
        WriteBarrierSegment* prev;
        WriteBarrier*        barrier;
        PrivateMemoryMapping mapping;
        bool                 primed; // Write protection status.
        size_t               increment_count;
        size_t               decrement_count;

        WriteBarrierSegment();

        WriteBarrierSegment(WriteBarrierSegment&&) = delete;
        WriteBarrierSegment(const WriteBarrierSegment&) = delete;
        WriteBarrierSegment& operator=(WriteBarrierSegment&&) = delete;
        WriteBarrierSegment& operator=(const WriteBarrierSegment&) = delete;

        Object** cursor();
        std::span<Object*> records();
        std::span<Object*> increment_records();
        std::span<Object*> decrement_records();
        std::span<std::byte> guard_page();
    };

    // A segmented object pointer vector. Bounds checking is performed indirectly
    // using the MMU (memory management unit) via guard pages at the end of segments.
    class WriteBarrier {
    public:
        using Phase = WriteBarrierPhase;

        explicit WriteBarrier(Ledger& ledger, size_t phase_shift);

        WriteBarrier(WriteBarrier&&) = delete;
        WriteBarrier(const WriteBarrier&) = delete;
        WriteBarrier& operator=(WriteBarrier&&) = delete;
        WriteBarrier& operator=(const WriteBarrier&) = delete;

        Ledger& ledger();

        [[nodiscard]]
        Phase phase() const;

        [[nodiscard]]
        bool is_empty() const;

        // TODO: Give these `_segment` suffixes.
        WriteBarrierSegment* back();
        void push_back(WriteBarrierSegment& segment);
        WriteBarrierSegment* pop_back();

        void commit();

        // NOTE: This is O(#segments).
        [[nodiscard]]
        size_t increment_count() const;

        // NOTE: This is O(#segments).
        [[nodiscard]]
        size_t decrement_count() const;

    private:
        Ledger&              ledger_;
        size_t               phase_shift_;
        WriteBarrierSegment* stack_; // Pointer to the top stack segment.
    };

    class WriteBarrierManager {
    public:
        [[nodiscard]]
        int file_descriptor();
        void poll(bool non_blocking);

        void attach(WriteBarrier& barrier);
        void detach(WriteBarrier& barrier);

        WriteBarrierSegment& allocate_segment();
        void deallocate_segment(WriteBarrierSegment& segment);

    private:
        void prime_guard_page(WriteBarrierSegment& segment);

    private:
        PageFaultHandler                                  page_fault_handler_;

        std::mutex                                        segment_pool_mutex_;
        std::vector<WriteBarrierSegment*>                 segment_pool_;
        std::vector<std::unique_ptr<WriteBarrierSegment>> segment_pool_storage_;
    };

    class Ledger {
    public:
        // Conceptually a `std::atomic<std::vector<Object*>::iterator>`.
        using Cursor = std::atomic<Object**>;

        static Cursor& local_increment_cursor() {
            thread_local Cursor cursor = nullptr;
            return cursor;
        }

        static Cursor& local_decrement_cursor() {
            thread_local Cursor cursor = nullptr;
            return cursor;
        }

        explicit Ledger(WriteBarrierManager& write_barrier_manager);
        ~Ledger();

        Ledger(Ledger&&) = delete;
        Ledger(const Ledger&) = delete;
        Ledger& operator=(Ledger&&) = delete;
        Ledger& operator=(const Ledger&) = delete;

        [[nodiscard]]
        Sequence sequence() const;

        [[nodiscard]]
        bool is_empty() const;

        Cursor& increment_cursor();
        Cursor& decrement_cursor();

        // Find the barrier in the corresponding phase.
        WriteBarrier& barrier(WriteBarrierPhase phase);

        // Advance all write barriers to the next phase.
        void commit();

    private:
        AtomicSequence       sequence_;
        Cursor&              increment_cursor_;
        Cursor&              decrement_cursor_;
        WriteBarrier         write_barriers_[WRITE_BARRIER_PHASE_COUNT];
        WriteBarrierManager& write_barrier_manager_;
    };

    MANTLE_HOT
    void increment_ref_cnt(Object* object) {
        std::atomic<Object**>& cursor = Ledger::local_increment_cursor();
        Object** record = cursor.load(std::memory_order_acquire); // Doesn't need to be a fetch-add.
        assert(record);

        cursor.store(record + 1, std::memory_order_release);
        *record = object; // Maybe null.
    }

    MANTLE_HOT
    void decrement_ref_cnt(Object* object) {
        std::atomic<Object**>& cursor = Ledger::local_decrement_cursor();
        Object** record = cursor.load(std::memory_order_acquire); // Doesn't need to be a fetch-add.
        assert(record);

        cursor.store(record + 1, std::memory_order_release);
        *record = object; // Maybe null.
    }

    using ObjectLedger = Ledger;

}


// include/mantle/object_cache.h


namespace mantle {

    // TODO: SSE2 probing.
    // TODO: Live mask that we can `&` with the probe result.
    //
    template<typename T, size_t CACHE_SIZE, size_t CACHE_WAYS>
    class ObjectCache {
    public:
        static_assert(is_power_of_2(CACHE_SIZE));
        static_assert(is_power_of_2(CACHE_WAYS));

        static constexpr size_t CACHE_SETS = CACHE_SIZE / CACHE_WAYS;

        static constexpr uintptr_t SET_SHIFT = log2_floor(alignof(Object));
        static constexpr uintptr_t SET_BITS  = log2_floor(CACHE_SETS);
        static constexpr uintptr_t SET_MASK  = (1ull << SET_BITS) - 1;

    public:
        struct Entry {
            Object* key;
            T       val;

            auto operator<=>(const Entry&) const noexcept = default;
        };

        class Cursor {
        public:
            explicit Cursor(const size_t pos = 0)
                : pos_(pos)
            {
                assert(pos_ <= CACHE_SIZE);
            }

            Cursor(const size_t set, const size_t way)
                : Cursor((set * CACHE_WAYS) + way)
            {
                assert(pos_ <= CACHE_SIZE);
            }

            auto operator<=>(const Cursor&) const noexcept = default;

            explicit operator bool() const {
                return pos_ < CACHE_SIZE;
            }

            [[nodiscard]]
            size_t set() const {
                return pos_ / CACHE_WAYS;
            }

            [[nodiscard]]
            size_t way() const {
                return pos_ % CACHE_WAYS;
            }

            std::optional<Cursor> next() const {
                if (Cursor cursor = *this; cursor.advance()) {
                    return cursor;
                }

                return std::nullopt;
            }

            bool advance() {
                assert(*this);

                pos_ += 1;
                return static_cast<bool>(*this);
            }

        private:
            size_t pos_;
        };

    public:
        ObjectCache() {
            reset();
        }

        std::pair<Cursor, Cursor> equal_range(Object* key) const {
            size_t set = to_set(key);

            return {
                Cursor(set, 0),
                Cursor(set + 1, 0),
            };
        }

        Entry load(Cursor cursor) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            return {
                .key = keys_[set][way],
                .val = vals_[set][way],
            };
        }

        void store(Cursor cursor, Entry entry) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            keys_[set][way] = entry.key;
            vals_[set][way] = entry.val;
        }

        void reset(Cursor cursor) {
            size_t set = cursor.set();
            size_t way = cursor.way();

            keys_[set][way] = nullptr;
            vals_[set][way] = T{};
        }

        void reset() {
            for (Cursor cursor; cursor; cursor.advance()) {
                reset(cursor);
            }
        }

    private:
        static size_t to_set(Object* key) {
            uintptr_t ptr;
            memcpy(&ptr, &key, sizeof(ptr));
            return (ptr >> SET_SHIFT) & SET_MASK;
        }

    private:
        Object* keys_[CACHE_SETS][CACHE_WAYS];
        T       vals_[CACHE_SETS][CACHE_WAYS];
    };

}


// include/mantle/doorbell.h


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


// include/mantle/message.h


#define MANTLE_MESSAGE_TYPES(X) \
    X(START)                    \
    X(ENTER)                    \
    X(SUBMIT)                   \
    X(RETIRE)                   \
    X(LEAVE)                    \

namespace mantle {

    enum class MessageType {
#define X(MANTLE_MESSAGE_TYPE) \
        MANTLE_MESSAGE_TYPE,   \

        MANTLE_MESSAGE_TYPES(X)
#undef X
    };

    // TODO: Make this a variant.
    union Message {
        MessageType type;

        // region -> domain
        struct Start {
            MessageType type;
        } start;

        // domain -> region
        struct Enter {
            MessageType type;
            Sequence    cycle;
        } enter;

        // region -> domain
        struct Submit {
            MessageType   type;
            bool          stop; // The region is ready to stop.
            WriteBarrier* write_barrier;
        } submit;

        // domain -> region
        struct Retire {
            MessageType  type;
            ObjectGroups garbage;
        } retire;

        // domain -> region
        struct Leave {
            MessageType type;
            bool        stop; // The domain is ready to stop.
        } leave;
    };

    constexpr Message make_start_message() {
        return {
            .start = {
                .type = MessageType::START,
            }
        };
    }

    constexpr Message make_enter_message(const Sequence cycle) {
        return {
            .enter = {
                .type  = MessageType::ENTER,
                .cycle = cycle,
            }
        };
    }

    constexpr Message make_leave_message(const bool stop) {
        return {
            .leave = {
                .type = MessageType::LEAVE,
                .stop = stop,
            }
        };
    }

    constexpr std::string_view to_string(const MessageType type) {
        using namespace std::literals;

        switch (type) {
#define X(MANTLE_MESSAGE_TYPE)                     \
            case MessageType::MANTLE_MESSAGE_TYPE: \
                return #MANTLE_MESSAGE_TYPE ##sv;  \

            MANTLE_MESSAGE_TYPES(X)
#undef X
        }

        abort();
    }

}


// include/mantle/ring.h


namespace mantle {

    template<typename T>
    class Ring {
    public:
        explicit Ring(size_t minimum_size) {
            size_t size = 1;
            while (size < minimum_size) {
                size = size * 2;
            }

            data_.resize(size);
            mask_ = size - 1;
        }

        size_t size() const {
            return data_.size();
        }

        T& operator[](Sequence sequence) {
            return data_[sequence & mask_];
        }

        const T& operator[](Sequence sequence) const {
            return data_[sequence & mask_];
        }

        void fill(const T& value) {
            for (size_t i = 0; i < data_.size(); ++i) {
                data_[i] = value;
            }
        }

    private:
        std::vector<T> data_;
        size_t         mask_;
    };

}


// include/mantle/operation_grouper.h


namespace mantle {

    struct OperationGrouperMetrics {
        size_t grouped_count           = 0;

        size_t written_count           = 0;
        size_t written_increment_count = 0;
        size_t written_decrement_count = 0;

        size_t flushed_count           = 0;
        size_t flushed_increment_count = 0;
        size_t flushed_decrement_count = 0;
    };

    // This class attempts to reduce the number of random memory writes needed to update reference counts
    // by combining operations on the same object into a single write. Grouped operations are not
    // imediately applied.
    //
    // This has two major benefits:
    //   1. Increments can be applied before decrements.
    //   2. The prefetcher should have an easier time predicting what will be touched next.
    //
    class OperationGrouper {
        static constexpr size_t CACHE_SIZE = 512;
        static constexpr size_t CACHE_WAYS = 8;

        struct OperationGroup {
            int64_t delta = 0;
            size_t  hit_count = 0;
            size_t  hit_decay = 0;
        };

        using Cache       = ObjectCache<OperationGroup, CACHE_SIZE, CACHE_WAYS>;
        using CacheEntry  = Cache::Entry;
        using CacheCursor = Cache::Cursor;

    public:
        using Metrics = OperationGrouperMetrics;

        OperationGrouper();

        [[nodiscard]]
        const Metrics& metrics() const;

        // Returns true if there are operations missing from the increment/decrement collections
        // because they have yet to be flushed.
        [[nodiscard]]
        bool is_dirty() const;

        [[nodiscard]]
        std::span<std::pair<Object*, int64_t>> increments();

        [[nodiscard]]
        std::span<std::pair<Object*, int64_t>> decrements();

        // Write an operation to the cache. If flush is true, the operation is immediately written to the
        // increment or decrement collection. Otherwise, the operation is grouped with other operations.
        void write(Operation operation, bool flush = false);

        // Flush operations from the cache to the increment and decrement collections.
        // The force parameter will cause all operations to be flushed regardless of the hit count,
        // which is useful during shutdown.
        void flush(bool force = false);

        // Clear the increment and decrement collections.
        void clear();

        // Equivelent to calling flush(true) and then clear().
        void reset();

    private:
        // Select a cache entry for this object using a bunch of heuristics.
        CacheCursor choose_way(Object* object);

        void flush_group(CacheCursor cursor, bool force);
        void reset_group(CacheCursor cursor);

        void note_operation_written(Operation operation);
        void note_operation_flushed(Operation operation);

    private:
        std::vector<std::pair<Object*, int64_t>> increments_;
        std::vector<std::pair<Object*, int64_t>> decrements_;
        size_t                                   cache_size_;
        Metrics                                  metrics_;
        Cache                                    cache_;
    };

}


// include/mantle/object_grouper.h


namespace mantle {

    struct ObjectGrouperMetrics {
        size_t      object_count = 0;
        ObjectGroup group_min = std::numeric_limits<ObjectGroup>::max();
        ObjectGroup group_max = std::numeric_limits<ObjectGroup>::min();
    };

    // This class groups objects for more efficient finalization.
    class ObjectGrouper {
    public:
        using Metrics = ObjectGrouperMetrics;

        ObjectGrouper()
            : group_min_(std::numeric_limits<ObjectGroup>::max())
            , group_max_(std::numeric_limits<ObjectGroup>::min())
        {
            for (size_t& bucket: group_buckets_) {
                bucket = 0;
            }
        }

        [[nodiscard]]
        const Metrics& metrics() const {
            return metrics_;
        }

        void write(Object& object) {
            const ObjectGroup group = object.group();

            group_buckets_[group] += 1;
            group_min_ = std::min(group, group_min_);
            group_max_ = std::max(group, group_max_);

            input_.push_back(&object);
        }

        [[nodiscard]]
        ObjectGroups flush() {
            ObjectGroups groups = {};

            metrics_.object_count += input_.size();
            metrics_.group_min = std::min(group_min_, metrics_.group_min);
            metrics_.group_max = std::max(group_max_, metrics_.group_max);

            if constexpr (MANTLE_ENABLE_OBJECT_GROUPING) {
                // Reset working memory.
                output_.resize(input_.size());
                for (size_t& offset: group_offsets_) {
                    offset = 0;
                }
                for (uint64_t& chunk: group_mask_) {
                    chunk = 0;
                }

                // Calculate group offsets and initialize the group mask.
                {
                    size_t offset = 0;
                    for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                        const size_t group_size = group_buckets_[group];
                        const uint64_t group_populated = !!group_size;

                        group_offsets_[group] = offset;
                        group_mask_[group / 64] |= (group_populated << (group % 64));

                        offset += group_size;
                    }

                    // The cumulative offset is stored at the end (not the back).
                    assert(offset == input_.size());
                    group_offsets_[static_cast<size_t>(group_max_) + 1] = offset;
                }

                // Group objects in O(n) using radix sort.
                for (Object* object: input_) {
                    const ObjectGroup group = object->group();

                    const size_t offset = group_offsets_[group];
                    size_t& bucket = group_buckets_[group];
                    assert(bucket);

                    bucket -= 1;
                    output_[offset + bucket] = object;
                }

                groups = ObjectGroups {
                    .objects       = output_.data(),
                    .object_count  = output_.size(),
                    .group_min     = group_min_,
                    .group_max     = group_max_,
                    .group_offsets = group_offsets_.data(),
                    .group_mask    = &group_mask_,
                };

#ifdef MANTLE_AUDIT
                // Sanity check group membership.
                for (ObjectGroup group = group_min_; group <= group_max_; ++group) {
                    const std::span<Object*> group_members = groups.group_members(group);
                    assert(group_members.size() <= groups.object_count);

                    for (const Object* object: group_members) {
                        assert(object->group() == group);
                        assert(object->is_managed());
                    }
                }
#endif
            }
            else {
                output_ = input_;

                groups = ObjectGroups {
                    .objects       = output_.data(),
                    .object_count  = output_.size(),
                    .group_min     = group_min_,
                    .group_max     = group_max_,
                    .group_offsets = nullptr,
                    .group_mask    = nullptr,
                };
            }

            input_.clear();
            group_min_ = std::numeric_limits<ObjectGroup>::max();
            group_max_ = std::numeric_limits<ObjectGroup>::min();
            for (size_t& bucket: group_buckets_) {
                bucket = 0;
            }

            return groups;
        }

    private:
        using GroupBucketArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 0>;
        using GroupOffsetArray = std::array<size_t, std::numeric_limits<ObjectGroup>::max() + 1>;

        std::vector<Object*> input_;
        ObjectGroup          group_min_;
        ObjectGroup          group_max_;
        GroupBucketArray     group_buckets_;

        std::vector<Object*> output_;
        GroupOffsetArray     group_offsets_;
        ObjectGroupMask      group_mask_;

        Metrics              metrics_;
    };

}


// include/mantle/connection.h


namespace mantle {

    class Stream {
        Stream(Stream&&) = delete;
        Stream(const Stream&) = delete;
        Stream& operator=(Stream&&) = delete;
        Stream& operator=(const Stream&) = delete;

    public:
        Stream(size_t minimum_capacity = MANTLE_STREAM_CAPACITY)
            : mask_()
            , head_(0)
            , tail_(0)
            , private_head_(0)
            , private_tail_(0)
        {
            size_t capacity = 1;
            while (capacity < minimum_capacity) {
                capacity *= 2;
            }

            mask_ = capacity - 1;
            ring_.resize(capacity);
        }

        size_t capacity() const {
            return ring_.size();
        }

        bool send(const Message& message) {
            uint64_t head = head_.load(std::memory_order_acquire);
            if ((private_tail_ - head) == ring_.size()) {
                return false; // Stream is full.
            }

            Slot& slot = ring_[private_tail_ & mask_];
            slot.message = message;

            private_tail_ += 1;
            tail_.store(private_tail_, std::memory_order_release);
            return true;
        }

        size_t receive(std::vector<Message>& messages) {
            Sequence tail = tail_.load(std::memory_order_acquire);
            size_t count = tail - private_head_;
            assert(count <= ring_.size());

            for (size_t i = 0; i < count; ++i) {
                messages.push_back(
                    ring_[(private_head_ + i) & mask_].message
                );
            }

            private_head_ += count;
            head_.store(private_head_, std::memory_order_release);
            return count;
        }

    private:
        struct alignas(MANTLE_CACHE_LINE_SIZE) Slot {
            Message message;
        };

        std::vector<Slot> ring_;
        size_t            mask_;

        alignas(MANTLE_CACHE_LINE_SIZE) AtomicSequence head_;
        alignas(MANTLE_CACHE_LINE_SIZE) AtomicSequence tail_;

        alignas(MANTLE_CACHE_LINE_SIZE) Sequence private_head_; // Private to receive.
        alignas(MANTLE_CACHE_LINE_SIZE) Sequence private_tail_; // Private to send.
    };

    class Endpoint {
        Endpoint(Endpoint&&) = delete;
        Endpoint(const Endpoint&) = delete;
        Endpoint& operator=(Endpoint&&) = delete;
        Endpoint& operator=(const Endpoint&) = delete;

    public:
        explicit Endpoint(Endpoint& remote_endpoint)
            : remote_endpoint_(remote_endpoint)
        {
            temp_messages_.reserve(stream_.capacity());
        }

        int file_descriptor() {
            return doorbell_.file_descriptor();
        }

        Stream& stream() {
            return stream_;
        }

        bool send_message(const Message& message) {
            if (!remote_endpoint_.stream_.send(message)) {
                return false;
            }

            remote_endpoint_.doorbell_.ring();
            return true;
        }

        std::span<const Message> receive_messages(bool non_blocking) {
            doorbell_.poll(non_blocking);

            temp_messages_.clear();
            size_t count = stream_.receive(temp_messages_);
            assert(count == temp_messages_.size());

            return {
                temp_messages_.data(),
                temp_messages_.size(),
            };
        }

    private:
        Endpoint&            remote_endpoint_;
        Doorbell             doorbell_;
        Stream               stream_;
        std::vector<Message> temp_messages_;
    };

    // A pair of endpoints linked with bidirectional message streams.
    class Connection {
    public:
        Connection()
            : client_endpoint_(server_endpoint_)
            , server_endpoint_(client_endpoint_)
        {
        }

        Endpoint& client_endpoint() {
            return client_endpoint_;
        }

        Endpoint& server_endpoint() {
            return server_endpoint_;
        }

    private:
        Endpoint client_endpoint_;
        Endpoint server_endpoint_;
    };

}


// include/mantle/region_controller.h


// Actions represents what is needed for the controller to advance.
//
// SEND:        Waiting to send a message to the associated `Region`.
// RECEIVE:     Waiting to receive a message from the associated `Region`.
// BARRIER_ANY: Any controller reaching this state causes all controllers to advance past it.
// BARRIER_ALL: All controllers must reach this state to advance past it.
//
#define MANTLE_REGION_CONTROLLER_ACTIONS(X) \
    X(SEND)                                 \
    X(RECEIVE)                              \
    X(BARRIER_ANY)                          \
    X(BARRIER_ALL)                          \

#define MANTLE_REGION_CONTROLLER_PHASES(X)  \
    X(START,          RECEIVE)              \
    X(START_BARRIER,  BARRIER_ANY)          \
    X(ENTER,          SEND)                 \
    X(SUBMIT,         RECEIVE)              \
    X(SUBMIT_BARRIER, BARRIER_ALL)          \
    X(RETIRE_BARRIER, BARRIER_ALL)          \
    X(RETIRE,         SEND)                 \
    X(LEAVE,          SEND)                 \

#define MANTLE_REGION_CONTROLLER_STATES(X) \
    X(STARTING)                            \
    X(RUNNING)                             \
    X(STOPPING)                            \
    X(STOPPED)                             \
    X(SHUTDOWN)                            \

namespace mantle {

    class Region;
    class RegionController;

    using RegionControllerGroup = std::vector<std::unique_ptr<RegionController>>;

    // What we are doing in this part of the cycle.
    enum class RegionControllerAction {
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                 \
        MANTLE_REGION_CONTROLLER_ACTION,                                   \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
    };

    // Which part of the cycle we are in.
    enum class RegionControllerPhase {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
        MANTLE_REGION_CONTROLLER_PHASE,                                    \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
    };

    // High level state related to starting and stopping.
    enum class RegionControllerState {
#define X(MANTLE_REGION_CONTROLLER_STATE) \
        MANTLE_REGION_CONTROLLER_STATE,   \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
    };

    constexpr size_t REGION_CONTROLLER_ACTION_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                 \
        + 1                                                                \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
    ;

    constexpr size_t REGION_CONTROLLER_PHASE_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
        + 1                                                                \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
    ;

    constexpr size_t REGION_CONTROLLER_STATE_COUNT = 0
#define X(MANTLE_REGION_CONTROLLER_STATE) \
        + 1                               \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
    ;

    // A survey of the states of controllers and the actions they are trying to take.
    class RegionControllerCensus {
    public:
        using State = RegionControllerState;
        using Phase = RegionControllerPhase;
        using Cycle = Sequence;
        using Action = RegionControllerAction;

        RegionControllerCensus();
        explicit RegionControllerCensus(const RegionController& controller);
        explicit RegionControllerCensus(const RegionControllerGroup& controllers);

        void add(const RegionController& controller);
        void add(const RegionControllerGroup& controllers);

        size_t count() const;

        Cycle min_cycle() const;
        Cycle max_cycle() const;

        bool any(State state) const;
        bool all(State state) const;

        bool any(Phase phase) const;
        bool all(Phase phase) const;
 
        bool any(Action action) const;
        bool all(Action action) const;

        auto operator<=>(const RegionControllerCensus&) const noexcept = default;

    private:
        size_t                                             count_;
        Cycle                                              min_cycle_;
        Cycle                                              max_cycle_;
        std::array<size_t, REGION_CONTROLLER_STATE_COUNT>  state_counts_;
        std::array<size_t, REGION_CONTROLLER_PHASE_COUNT>  phase_counts_;
        std::array<size_t, REGION_CONTROLLER_ACTION_COUNT> action_counts_;
    };

    struct RegionControllerMetrics {
        const OperationGrouper::Metrics& operation_grouper;
        const ObjectGrouper::Metrics& object_grouper;
        size_t increment_count;
        size_t decrement_count;

        RegionControllerMetrics(
            const OperationGrouper& operation_grouper,
            const ObjectGrouper& object_grouper
        )
            : operation_grouper(operation_grouper.metrics())
            , object_grouper(object_grouper.metrics())
            , increment_count(0)
            , decrement_count(0)
        {
        }
    };

    // The `Domain` creates one of these for each bound `Region`.
    // It is responsible for driving the synchronization mechanism
    // between the associated `Region` and peer `RegionController`'s.
    // Controllers can be in different states, and synchronize among
    // themselves at barrier states as needed.
    //
    class RegionController {
    public:
        using State   = RegionControllerState;
        using Phase   = RegionControllerPhase;
        using Cycle   = Sequence;
        using Action  = RegionControllerAction;
        using Metrics = RegionControllerMetrics;

        RegionController(
            RegionId region_id,
            RegionControllerGroup& controllers,
            WriteBarrierManager& write_barrier_manager
        );

        RegionController(RegionController&&) = delete;
        RegionController(const RegionController&) = delete;
        RegionController& operator=(RegionController&&) = delete;
        RegionController& operator=(const RegionController&) = delete;

        RegionId region_id() const;
        const Metrics& metrics() const;

        bool is_quiescent() const;

        State state() const;
        Phase phase() const;
        Cycle cycle() const;
        Action action() const;

        void start(Cycle cycle);
        void stop();

        std::optional<Message> send_message();
        void receive_message(const Message& message);
        void synchronize(const RegionControllerCensus& census);

    private:
        void transition(State next_state);
        void transition(Phase next_phase);
        void transition(Cycle next_cycle);

        size_t route_operations(OperationType type, std::span<Object*> objects);

    private:
        RegionId               region_id_;
        RegionControllerGroup& controllers_;
        WriteBarrierManager&   write_barrier_manager_;

        State                  state_;
        Phase                  phase_;
        Cycle                  cycle_;

        WriteBarrier*          write_barrier_;
        OperationGrouper       operation_grouper_;
        ObjectGrouper          object_grouper_;

        Metrics                metrics_;
    };

    // Synchronizes a group of region controllers.
    RegionControllerCensus synchronize(RegionControllerGroup& controllers);

    std::string_view to_string(RegionControllerState state);
    std::string_view to_string(RegionControllerPhase phase);
    std::string_view to_string(RegionControllerAction action);

}


// include/mantle/region.h


#define MANTLE_REGION_STATES(X) \
    X(RUNNING)                  \
    X(STOPPING)                 \
    X(STOPPED)                  \

#define MANTLE_REGION_PHASES(X) \
    X(RECV_ENTER)               \
    X(RECV_ENTER_SENT_START)    \
    X(RECV_RETIRE)              \
    X(RECV_LEAVE)               \

namespace mantle {

    class Domain;
    class Object;
    class Finalizer;

    enum class RegionState {
#define X(MANTLE_REGION_STATE) \
        MANTLE_REGION_STATE,   \

        MANTLE_REGION_STATES(X)
#undef X
    };

    enum class RegionPhase {
#define X(MANTLE_REGION_PHASE) \
        MANTLE_REGION_PHASE,   \

        MANTLE_REGION_PHASES(X)
#undef X
    };

    class Region {
    public:
        using State = RegionState;
        using Phase = RegionPhase;
        using Cycle = Sequence;

        Region(Domain& domain, Finalizer& finalizer);
        ~Region();

        Region(Region&&) = delete;
        Region(const Region&) = delete;
        Region& operator=(Region&&) = delete;
        Region& operator=(const Region&) = delete;

        RegionId id() const;

        State state() const;
        Phase phase() const;
        Cycle cycle() const;

        // Call step when this becomes readable.
        int file_descriptor();

        void stop();
        void step(bool non_blocking);

    private:
        friend class Domain;

        Endpoint& domain_endpoint();
        Endpoint& region_endpoint();

    private:
        void handle_message(const Message& message);

        void transition(State next_state);
        void transition(Phase next_phase);
        void transition(Cycle next_cycle);

        void finalize_garbage();

    private:
        static constexpr State INITIAL_STATE = State::RUNNING;
        static constexpr Phase INITIAL_PHASE = Phase::RECV_ENTER;
        static constexpr Cycle INITIAL_CYCLE = 0;

        Domain&                     domain_;
        RegionId                    id_;

        State                       state_;
        Phase                       phase_;
        Cycle                       cycle_;
        size_t                      depth_;

        Connection                  connection_;
        Finalizer&                  finalizer_;
        Ledger                      ledger_;
        std::optional<ObjectGroups> garbage_;

    public:
        static Region*& thread_local_instance() {
            thread_local Region* instance = nullptr;
            return instance;
        }
    };

    std::string_view to_string(RegionState state);
    std::string_view to_string(RegionPhase phase);

}


// include/mantle/domain.h


namespace mantle {

    class Region;

    class Domain {
        friend class Region;

    public:
        explicit Domain(std::optional<std::span<size_t>> thread_cpu_affinities = std::nullopt);
        ~Domain();

        Domain(Domain&&) = delete;
        Domain(const Domain&) = delete;
        Domain& operator=(Domain&&) = delete;
        Domain& operator=(const Domain&) = delete;

        [[nodiscard]]
        WriteBarrierManager& write_barrier_manager();

    private:
        void run();

        void handle_event(void* user_data);

        void update_controllers(const RegionControllerCensus& census);
        void start_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);
        void stop_controllers(const RegionControllerCensus& census, std::scoped_lock<std::mutex>&);

        RegionId bind(Region& region);

    private:
        std::thread            thread_;

        std::mutex             regions_mutex_;
        std::vector<Region*>   regions_;
        RegionControllerGroup  controllers_;

        WriteBarrierManager    write_barrier_manager_;

        bool                   running_;
        Doorbell               doorbell_;
        Selector               selector_;
    };

}


// include/mantle/ref.h


namespace mantle {

    template<typename T>
    class Ref;

    template<typename T>
    class Ptr;

    template<typename T>
    Ref<T> bind(T& object) noexcept;

    template<typename T>
    Ptr<T> bind(T* object) noexcept;

    // A reference to an object that allows access and will keep it alive while at least
    // one reference exists to the object. References cannot be null (like normal C++ references).
    //
    template<typename T>
    class Ref {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        template<typename U>
        friend class Ref;

        template<typename U>
        friend class Ptr;

        friend Ref<T> bind<T>(T& object) noexcept;

        Ref(T& object)
            : object_(&object)
        {
            Region* region = Region::thread_local_instance();
            assert(region);

            static_cast<Object*>(object_)->bind(region->id());
        }

    public:
        Ref() = delete;

        Ref(const Ref& other) noexcept
            : object_(other.object_)
        {
            increment_ref_cnt(object_);
        }

        template<typename U>
        Ref(const Ref<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        template<typename U>
        Ref(const Ptr<U>& other);

        Ref& operator=(const Ref& that) noexcept {
            // We don't need to check if `this != that`.
            // The increment will be reordered before the decrement.
            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ref& operator=(const Ref<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ref& operator=(const Ptr<U>& that);

        ~Ref() noexcept {
            decrement_ref_cnt(object_);
        }

        T* get() noexcept {
            return object_;
        }

        const T* get() const noexcept {
            return object_;
        }

        T& operator*() noexcept {
            return *object_;
        }

        const T& operator*() const noexcept {
            return *object_;
        }

        T* operator->() noexcept {
            return object_;
        }

        const T* operator->() const noexcept {
            return object_;
        }

    private:
        T* object_;
    };

    // Pointers are nullable references. They also keep the object, pointed to alive (i.e. not weak).
    // This is in an improvement over `std::optional<Ref<T>>` in a couple of ways:
    //   1. `sizeof(Ptr<T>) < sizeof(std::optional<T>)`.
    //   2. More efficient copying (branchless).
    //   3. Automatic conversion to references with null checking.
    //
    template<typename T>
    class Ptr {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        template<typename U>
        friend class Ref;

        template<typename U>
        friend class Ptr;

        friend Ptr<T> bind<T>(T* object) noexcept;

        Ptr(T* object)
            : object_(object)
        {
            if (object_) {
                Region* region = Region::thread_local_instance();
                assert(region);

                static_cast<Object*>(object_)->bind(region->id());
            }
        }

    public:
        Ptr() noexcept
            : object_(nullptr)
        {
        }

        Ptr(const Ptr& other) noexcept
            : object_(other.object_)
        {
            increment_ref_cnt(object_);
        }

        template<typename U>
        Ptr(const Ptr<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        template<typename U>
        Ptr(const Ref<U>& other) noexcept
            : object_(other.object_)
        {
            static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.

            increment_ref_cnt(object_);
        }

        Ptr& operator=(const Ptr& that) noexcept {
            // We don't need to check if `this != that`.
            // The increment will be reordered before the decrement.
            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ptr& operator=(const Ptr<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        template<typename U>
        Ptr& operator=(const Ref<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            decrement_ref_cnt(object_);
            object_ = that.object_;
            increment_ref_cnt(object_);

            return *this;
        }

        ~Ptr() noexcept {
            reset();
        }

        T* get() noexcept {
            return object_;
        }

        const T* get() const noexcept {
            return object_;
        }

        T& operator*() noexcept {
            return *object_;
        }

        const T& operator*() const noexcept {
            return *object_;
        }

        T* operator->() noexcept {
            return object_;
        }

        const T* operator->() const noexcept {
            return object_;
        }

        explicit operator bool() const {
            return static_cast<bool>(object_);
        }

        void reset() {
            decrement_ref_cnt(release());
        }

        T* release() {
            return std::exchange(object_, nullptr);
        }

        void acquire(T* object) {
            assert(!object || object->is_managed());

            decrement_ref_cnt(object_);
            object_ = object;
        }

    private:
        T* object_;
    };

    template<typename T>
    template<typename U>
    Ref<T>::Ref(const Ptr<U>& other)
        :  object_(other.object_)
    {
        static_assert(std::is_base_of_v<T, U>); // TODO: lift this into a concept.
        if (!object_) {
            throw std::runtime_error("Null reference");
        }

        increment_ref_cnt(object_);
    }

    template<typename T>
    template<typename U>
    Ref<T>& Ref<T>::operator=(const Ptr<U>& that) {
        static_assert(std::is_base_of_v<T, U>);
        if (!that.object_) {
            throw std::runtime_error("Null reference");
        }

        decrement_ref_cnt(object_);
        object_ = that.object_;
        increment_ref_cnt(object_);

        return *this;
    }

    template<typename T>
    inline Ref<T> bind(T& object) noexcept {
        return Ref<T>(object);
    }

    template<typename T>
    inline Ptr<T> bind(T* object) noexcept {
        return Ptr<T>(object);
    }

}

namespace std {

    // TODO: Specialize std::atomic to make it `is_lock_free`.
    // template<typename T>
    // class atomic<Ref<T>> {
    // public:
    // };
    // template<typename T>
    // class atomic<Ptr<T>> {
    // public:
    // };

}


// include/mantle/finalizer.h


namespace mantle {

    // An interface for cleaning up objects once they are no longer referenced.
    class Finalizer {
    public:
        virtual ~Finalizer() = default;

        // Objects are finalized in batches based on group membership.
        virtual void finalize(ObjectGroup group, std::span<Object*> objects) noexcept = 0;
    };

}


// include/mantle/debug.h


#define MANTLE_INFO  0
#define MANTLE_DEBUG 0
#define MANTLE_AUDIT 0

namespace mantle {

    inline std::ostream& operator<<(std::ostream& stream, const Operation& operation) {
        Operation mutable_operation = operation;

        std::stringstream ss;
        ss << "Operation(object:" << static_cast<const void*>(mutable_operation.object());
        ss << ", value:" << static_cast<int>(mutable_operation.value()) << ")";

        stream << ss.str();
        return stream;
    }

    inline std::ostream& operator<<(std::ostream& stream, const RegionControllerGroup& controllers) {
        std::stringstream ss;
        ss << "RegionControllerGroup(\n";
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            auto&& controller = *controllers[region_id];

            ss << "  RegionController(id:" << region_id;
            ss << ", phase:" << to_string(controller.phase());
            ss << ", action:" << to_string(controller.action()) << ")\n";
        }
        ss << ")";

        stream << ss.str();
        return stream;
    }

    template<typename... Args>
    inline void debug(const char* fmt, Args&&... args) {
#if MANTLE_DEBUG
        std::string log_line = fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...) + '\n';
        ssize_t count = write(1, log_line.c_str(), log_line.size());
        (void)count;
#else
        (void)fmt;
        ((void)args, ...); // This is ridiculous. Turn off the warning.
#endif
    }

    template<typename... Args>
    inline void info(const char* fmt, Args&&... args) {
#if MANTLE_INFO
        std::string log_line = fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...) + '\n';
        ssize_t count = write(1, log_line.c_str(), log_line.size());
        (void)count;
#else
        (void)fmt;
        ((void)args, ...); // This is ridiculous. Turn off the warning.
#endif
    }

    template<typename... Args>
    inline void warning(const char* fmt, Args&&... args) {
#if MANTLE_INFO
        std::string log_line = fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...) + '\n';
        ssize_t count = write(1, log_line.c_str(), log_line.size());
        (void)count;
#else
        (void)fmt;
        ((void)args, ...); // This is ridiculous. Turn off the warning.
#endif
    }

}


// include/mantle/mantle.h



// src/region.cpp

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

inline
    Region::Region(Domain& domain, Finalizer& finalizer)
        : domain_(domain)
        , id_(std::numeric_limits<RegionId>::max())
        , state_(INITIAL_STATE)
        , phase_(INITIAL_PHASE)
        , cycle_(INITIAL_CYCLE)
        , depth_(0)
        , finalizer_(finalizer)
        , ledger_(domain.write_barrier_manager())
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

inline
    Region::~Region() {
        stop();

        thread_local_instance() = nullptr;
    }

inline
    RegionId Region::id() const {
        return id_;
    }

inline
    auto Region::state() const -> State {
        return state_;
    }

inline
    auto Region::phase() const -> Phase {
        return phase_;
    }

inline
    auto Region::cycle() const -> Cycle {
        return cycle_;
    }

inline
    int Region::file_descriptor() {
        return connection_.client_endpoint().file_descriptor();
    }

inline
    void Region::stop() {
        if (state_ != State::RUNNING) {
            return;
        }

        // Flag that we want to stop and participate until the domain
        // indicates that it is safe to do so.
        state_ = State::STOPPING;
        while (state_ != State::STOPPED) {
            constexpr bool non_blocking = false;
            step(non_blocking);
        }
    }

inline
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

inline
    Endpoint& Region::domain_endpoint() {
        return connection_.server_endpoint();
    }

inline
    Endpoint& Region::region_endpoint() {
        return connection_.client_endpoint();
    }

inline
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

inline
    void Region::transition(const State next_state) {
        if (state_ == next_state) {
            return;
        }

        debug("[region:{}] transition state {} to {}", id_, to_string(state_), to_string(next_state));
        state_ = next_state;
    }

inline
    void Region::transition(const Phase next_phase) {
        if (phase_ == next_phase) {
            return;
        }

        debug("[region:{}] transition phase {} to {}", id_, to_string(phase_), to_string(next_phase));
        phase_ = next_phase;
    }

inline
    void Region::transition(const Cycle next_cycle) {
        if (cycle_ == next_cycle) {
            return;
        }

        debug("[region:{}] transition cycle {} to {}", id_, cycle_, next_cycle);
        cycle_ = next_cycle;
    }

inline
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

inline
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

inline
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


// src/operation_grouper.cpp

namespace mantle {

inline
    OperationGrouper::OperationGrouper()
        : cache_size_(0)
    {
    }

inline
    auto OperationGrouper::metrics() const -> const Metrics& {
        return metrics_;
    }

inline
    bool OperationGrouper::is_dirty() const {
        return cache_size_ > 0;
    }

inline
    std::span<std::pair<Object*, int64_t>> OperationGrouper::increments() {
        return increments_;
    }

inline
    std::span<std::pair<Object*, int64_t>> OperationGrouper::decrements() {
        return decrements_;
    }

inline
    void OperationGrouper::write(const Operation operation, const bool flush) {
        Object* object = operation.mutable_object();

        // Ignore no-ops.
        if (UNLIKELY(!object)) {
            return;
        }

        if (flush) {
            // Bypass the cache and immediately flush the operation.
            // The operation doesn't need to be re-encoded which makes this
            // much simpler than flushing an operation group.
            if (operation.type() == OperationType::INCREMENT) {
                increments_.emplace_back(object, operation.value());
            }
            else {
                decrements_.emplace_back(object, operation.value());
            }
        }
        else {
            CacheCursor cursor = choose_way(object);
            CacheEntry entry = cache_.load(cursor);

            if (entry.key == object) {
                // Update an existing group.
                entry.val.delta += operation.value();
                entry.val.hit_count += 1;
                if (entry.val.delta) {
                    cache_.store(cursor, entry);
                }
                else {
                    cache_.reset(cursor);
                    cache_size_ -= 1;
                }
            }
            else if (entry.key) {
                // Replace an existing group.
                const bool force = true;
                flush_group(cursor, force);

                cache_.store(cursor, CacheEntry {
                    .key = object,
                    .val = {
                        .delta     = operation.value(),
                        .hit_count = 0,
                        .hit_decay = 1,
                    },
                });

                cache_size_ += 1;
            }
            else {
                // Insert a new group.
                cache_.store(cursor, CacheEntry {
                    .key = object,
                    .val = {
                        .delta     = operation.value(),
                        .hit_count = 0,
                        .hit_decay = 1,
                    },
                });

                cache_size_ += 1;
            }

            note_operation_written(operation);
        }
    }

inline
    void OperationGrouper::flush(const bool force) {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            flush_group(cursor, force);
        }
    }

inline
    void OperationGrouper::clear() {
        increments_.clear();
        decrements_.clear();
    }

inline
    void OperationGrouper::reset() {
        for (CacheCursor cursor; cursor; cursor.advance()) {
            reset_group(cursor);
        }
        assert(cache_size_ == 0);

        clear();
    }

inline
    auto OperationGrouper::choose_way(Object* object) -> CacheCursor {
        // Find the set that maps to this object.
        std::pair<CacheCursor, CacheCursor> set = cache_.equal_range(object);

        // Check if an entry for the object already exists in the set.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            if (auto&& [key, _] = cache_.load(cursor); key == object) {
                return cursor;
            }
        }

        // Look for an empty entry.
        for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
            if (auto&& [key, _] = cache_.load(cursor); !key) {
                return cursor;
            }
        }

        // Find the entry with the lowest delta magnitude. Break ties by choosing the lowest way.
        {
            CacheCursor min_cursor = set.first;
            int64_t min_delta_magnitude = std::numeric_limits<int64_t>::max();

            for (CacheCursor cursor = set.first; cursor != set.second; cursor.advance()) {
                auto&& [key, group] = cache_.load(cursor);

                const int64_t delta = group.delta;
                const int64_t delta_magnitude = delta < 0 ? -delta : +delta;

                if (delta_magnitude < min_delta_magnitude) {
                    min_cursor = cursor;
                    min_delta_magnitude = delta_magnitude;
                }
            }

            return min_cursor;
        }
    }

inline
    void OperationGrouper::flush_group(const CacheCursor cursor, const bool force) {
        auto&& [key, group] = cache_.load(cursor);
        if (!key) {
            return;
        }

        // Operation groups need an exponential number of hits to avoid being flushed.
        group.hit_decay *= 2;
        if (group.hit_decay < group.hit_count && !force) {
            return; // Seems active, keep this group alive for now.
        }

        std::vector<std::pair<Object*, int64_t>>& collection = group.delta >= 0 ? increments_ : decrements_;
        collection.emplace_back(key, group.delta);

        reset_group(cursor);
    }

inline
    void OperationGrouper::reset_group(const CacheCursor cursor) {
        if (auto&& [key, _] = cache_.load(cursor); key) {
            assert(cache_size_ > 0);
            cache_.reset(cursor);
            cache_size_ -= 1;
        }
    }

inline
    void OperationGrouper::note_operation_written(const Operation operation) {
        metrics_.written_count += 1;

        switch (operation.type()) {
            case OperationType::INCREMENT: {
                metrics_.written_increment_count += 1;
                break;
            }
            case OperationType::DECREMENT: {
                metrics_.written_decrement_count += 1;
                break;
            }
        }
    }

inline
    void OperationGrouper::note_operation_flushed(const Operation operation) {
        metrics_.flushed_count += 1;

        switch (operation.type()) {
            case OperationType::INCREMENT: {
                metrics_.flushed_increment_count += 1;
                break;
            }
            case OperationType::DECREMENT: {
                metrics_.flushed_decrement_count += 1;
                break;
            }
        }
    }

}


// src/object.cpp

namespace mantle {

inline
    Object::Object(ObjectGroup group)
        : reference_count_(0)
        , region_id_(INVALID_REGION_ID)
        , group_(group)
    {
    }

inline
    Object::~Object() {
#if MANTLE_AUDIT
        bool halting = !has_region();
        bool dropped = reference_count_ == 0;

        assert(halting || dropped);
#endif
    }

inline
    bool Object::is_managed() const {
        return region_id_ != INVALID_REGION_ID;
    }

inline
    RegionId Object::region_id() const {
        return region_id_;
    }

inline
    ObjectGroup Object::group() const {
        return group_;
    }

inline
    void Object::bind(RegionId region_id) {
        if (UNLIKELY(is_managed())) {
            abort(); // Don't bind an object more than once.
        }

        region_id_ = region_id;
    }

inline
    bool Object::apply_increment(const uint32_t delta_magnitude) {
        reference_count_ += delta_magnitude;
        return true;
    }

inline
    bool Object::apply_decrement(const uint32_t delta_magnitude) {
        if (reference_count_ < delta_magnitude) {
            reference_count_ = 0;
            region_id_ = INVALID_REGION_ID;
            return false;
        }

        reference_count_ -= delta_magnitude;
        return true;
    }

}


// src/ledger.cpp

namespace mantle {

inline
    PrivateMemoryMapping::PrivateMemoryMapping(const size_t size, const bool populate) {
        assert(size >= PAGE_SIZE);
        assert((size % PAGE_SIZE) == 0);

        void* address = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (address == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }

        memory_ = std::span(static_cast<std::byte*>(address), size);

        // Populate the mapping by pre-faulting every backing page by writing a zero to it.
        //
        // FIXME: Do this better, with a side effect. Compilers will probably optimize this out as is.
        //
        if (populate) {
            for (size_t i = 0; i < memory_.size_bytes(); i += PAGE_SIZE) {
                const_cast<volatile std::byte&>(memory_[i]) = std::byte{0};
            }
        }
    }

inline
    PrivateMemoryMapping::~PrivateMemoryMapping() {
        const int result = munmap(memory_.data(), memory_.size());
        assert(result >= 0);
    }

inline
    std::span<std::byte> PrivateMemoryMapping::memory() {
        return memory_;
    }

inline
    std::span<const std::byte> PrivateMemoryMapping::memory() const {
        return memory_;
    }

inline
    WriteBarrierSegment::WriteBarrierSegment()
        : prev(nullptr)
        , barrier(nullptr)
        , mapping(MANTLE_WRITE_BARRIER_SEGMENT_CAPACITY * sizeof(Object*), true)
        , primed(false)
        , increment_count(0)
        , decrement_count(0)
    {
    }

inline
    Object** WriteBarrierSegment::cursor() {
        Object** base = reinterpret_cast<Object**>(mapping.memory().data());
        return base + (increment_count + decrement_count);
    }

inline
    std::span<Object*> WriteBarrierSegment::records() {
        return {
            reinterpret_cast<Object**>(mapping.memory().data()),
            increment_count + decrement_count
        };
    }

inline
    std::span<Object*> WriteBarrierSegment::increment_records() {
        return records().last(increment_count);
    }

inline
    std::span<Object*> WriteBarrierSegment::decrement_records() {
        return records().first(decrement_count);
    }

inline
    std::span<std::byte> WriteBarrierSegment::guard_page() {
        return mapping.memory().last(PAGE_SIZE);
    }

inline
    WriteBarrier::WriteBarrier(Ledger& ledger, const size_t phase_shift)
        : ledger_(ledger)
        , phase_shift_(phase_shift)
        , stack_(nullptr)
    {
        assert(phase_shift_ < WRITE_BARRIER_PHASE_COUNT);
    }

inline
    Ledger& WriteBarrier::ledger() {
        return ledger_;
    }

inline
    auto WriteBarrier::phase() const -> Phase {
        return static_cast<Phase>((ledger_.sequence() + phase_shift_) % WRITE_BARRIER_PHASE_COUNT);
    }

inline
    bool WriteBarrier::is_empty() const {
        if (increment_count() || decrement_count()) {
            return false;
        }

        // Check if there are any non-committed writes.
        if (stack_) {
            switch (phase()) {
                case Phase::STORE_DECREMENTS: {
                    if (stack_->cursor() != ledger_.decrement_cursor().load(std::memory_order_acquire)) {
                        return false;
                    }
                    break;
                }
                case Phase::STORE_INCREMENTS: {
                    if (stack_->cursor() != ledger_.increment_cursor().load(std::memory_order_acquire)) {
                        return false;
                    }
                    break;
                }
                default: {
                    break;
                }
            }
        }

        return true;
    }

inline
    WriteBarrierSegment* WriteBarrier::back() {
        return stack_;
    }

inline
    void WriteBarrier::push_back(WriteBarrierSegment& segment) {
        assert(!segment.barrier);
        assert(!segment.prev);
        assert(segment.increment_count == 0);
        assert(segment.decrement_count == 0);
        assert(segment.primed);

        segment.barrier = this;
        segment.prev = stack_;

        switch (phase()) {
            case WriteBarrierPhase::STORE_INCREMENTS: {
                ledger_.increment_cursor().store(segment.cursor(), std::memory_order_release);
                break;
            }
            case WriteBarrierPhase::STORE_DECREMENTS: {
                ledger_.decrement_cursor().store(segment.cursor(), std::memory_order_release);
                break;
            }
            default: {
                break; // This segment is not active.
            }
        }

        stack_ = &segment;
    }

inline
    WriteBarrierSegment* WriteBarrier::pop_back() {
        if (!stack_) {
            return nullptr;
        }

        switch (phase()) {
            case WriteBarrierPhase::STORE_INCREMENTS: {
                ledger_.increment_cursor().store(nullptr, std::memory_order_release);
                break;
            }
            case WriteBarrierPhase::STORE_DECREMENTS: {
                ledger_.decrement_cursor().store(nullptr, std::memory_order_release);
                break;
            }
            default: {
                break; // This segment is not active.
            }
        }

        return std::exchange(stack_, stack_->prev);
    }

inline
    void WriteBarrier::commit() {
        assert(stack_);

        switch (phase()) {
            case Phase::STORE_INCREMENTS: {
                auto first = stack_->cursor();
                auto last = ledger_.increment_cursor().load(std::memory_order_acquire);
                stack_->increment_count = last - first;
                break;
            }
            case Phase::STORE_DECREMENTS: {
                auto first = stack_->cursor();
                auto last = ledger_.decrement_cursor().load(std::memory_order_acquire);
                stack_->decrement_count = last - first;
                break;
            }
            default: {
                abort();
            }
        }
    }

inline
    size_t WriteBarrier::increment_count() const {
        size_t count = 0;

        for (const WriteBarrierSegment* segment = stack_; segment; segment = segment->prev) {
            count += segment->increment_count;
        }

        return count;
    }

inline
    size_t WriteBarrier::decrement_count() const {
        size_t count = 0;

        for (const WriteBarrierSegment* segment = stack_; segment; segment = segment->prev) {
            count += segment->decrement_count;
        }

        return count;
    }

inline
    int WriteBarrierManager::file_descriptor() {
        return page_fault_handler_.file_descriptor();
    }

inline
    void WriteBarrierManager::poll(const bool non_blocking) {
        page_fault_handler_.poll([this](std::span<const std::byte> memory, PageFaultHandler::Mode mode) {
            if (mode == PageFaultHandler::Mode::WRITE_PROTECT) {
                WriteBarrierSegment* prev_segment;
                memcpy(&prev_segment, memory.data(), sizeof(prev_segment));

                WriteBarrier& barrier = *prev_segment->barrier;
                barrier.commit();

                WriteBarrierSegment& next_segment = allocate_segment();
                assert(next_segment.primed);
                barrier.push_back(next_segment);

                // Allow the pending write to proceed now that the next segment has been installed.
                prev_segment->primed = false;
                page_fault_handler_.write_unprotect_memory(prev_segment->guard_page());
            }
            else {
                abort();
            }
        }, non_blocking);
    }

inline
    void WriteBarrierManager::attach(WriteBarrier& barrier) {
        WriteBarrierSegment& segment = allocate_segment();
        barrier.push_back(segment);
    }

inline
    void WriteBarrierManager::detach(WriteBarrier& barrier) {
        while (WriteBarrierSegment* segment = barrier.pop_back()) {
            deallocate_segment(*segment);
        }
    }

inline
    void WriteBarrierManager::prime_guard_page(WriteBarrierSegment& segment) {
        if (segment.primed) {
            return;
        }

        const WriteBarrierSegment* segment_address = &segment;
        std::span<std::byte> guard_page = segment.guard_page();
        memcpy(guard_page.data(), &segment_address, sizeof(segment_address));

        page_fault_handler_.write_protect_memory(guard_page);
        segment.primed = true;
    }

inline
    WriteBarrierSegment& WriteBarrierManager::allocate_segment() {
        std::scoped_lock lock(segment_pool_mutex_);

        WriteBarrierSegment* segment = nullptr;
        if (UNLIKELY(segment_pool_.empty())) {
            segment = segment_pool_storage_.emplace_back(std::make_unique<WriteBarrierSegment>()).get();
            page_fault_handler_.register_memory(segment->guard_page(), {PageFaultHandler::Mode::WRITE_PROTECT});
        }
        else {
            segment = segment_pool_.back();
            segment_pool_.pop_back();
        }

        prime_guard_page(*segment);
        return *segment;
    }

inline
    void WriteBarrierManager::deallocate_segment(WriteBarrierSegment& segment) {
        std::scoped_lock lock(segment_pool_mutex_);

        segment.barrier = nullptr;
        segment.prev = nullptr;
        segment.increment_count = 0;
        segment.decrement_count = 0;

        // TODO: Limit the maximum capacity of this (with a config parameter).
        segment_pool_.push_back(&segment);
    }

inline
    Ledger::Ledger(WriteBarrierManager& write_barrier_manager)
        : sequence_(0)
        , increment_cursor_(local_increment_cursor())
        , decrement_cursor_(local_decrement_cursor())
        , write_barriers_{
            WriteBarrier{*this, 0},
            WriteBarrier{*this, 1},
            WriteBarrier{*this, 2},
            WriteBarrier{*this, 3},
        }
        , write_barrier_manager_(write_barrier_manager)
    {
        for (auto&& barrier : write_barriers_) {
            write_barrier_manager_.attach(barrier);
        }
    }

inline
    Ledger::~Ledger() {
        for (auto&& barrier : write_barriers_) {
            write_barrier_manager_.detach(barrier);
        }
    }

inline
    Sequence Ledger::sequence() const {
        return sequence_.load(std::memory_order_acquire);
    }

    bool Ledger::is_empty() const {
        for (const WriteBarrier& barrier : write_barriers_) {
            if (!barrier.is_empty()) {
                return false;
            }
        }

        return true;
    }

inline
    auto Ledger::increment_cursor() -> Cursor& {
        return increment_cursor_;
    }

inline
    auto Ledger::decrement_cursor() -> Cursor& {
        return decrement_cursor_;
    }

inline
    WriteBarrier& Ledger::barrier(const WriteBarrierPhase phase) {
        const Sequence sequence = sequence_.load(std::memory_order_acquire);

        WriteBarrier& barrier = write_barriers_[(static_cast<uint64_t>(phase) - sequence) % WRITE_BARRIER_PHASE_COUNT];
        assert(phase == barrier.phase());
        return barrier;
    }

inline
    void Ledger::commit() {
        // Commit inc/dec writes.
        {
            WriteBarrier& increment_barrier = barrier(WriteBarrierPhase::STORE_INCREMENTS);
            WriteBarrier& decrement_barrier = barrier(WriteBarrierPhase::STORE_DECREMENTS);

            increment_barrier.commit();
            decrement_barrier.commit();
        }

        // Atomically advance all write barriers to the next phase.
        // Their phase is determined by the current sequence number.
        sequence_.fetch_add(1, std::memory_order_acq_rel);

        // Setup the new inc/dec barriers to receive subsequent writes.
        {
            WriteBarrier& increment_barrier = barrier(WriteBarrierPhase::STORE_INCREMENTS);
            WriteBarrier& decrement_barrier = barrier(WriteBarrierPhase::STORE_DECREMENTS);

            increment_cursor_.store(increment_barrier.back()->cursor(), std::memory_order_release);
            decrement_cursor_.store(decrement_barrier.back()->cursor(), std::memory_order_release);
        }
    }

}


// src/region_controller.cpp

namespace mantle {

    inline RegionControllerAction to_action(RegionControllerPhase phase) {
        switch (phase) {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION)      \
            case RegionControllerPhase::MANTLE_REGION_CONTROLLER_PHASE: {       \
                return RegionControllerAction::MANTLE_REGION_CONTROLLER_ACTION; \
            }                                                                   \

            MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
        }

        abort(); // Unreachable.
    }

    inline RegionControllerPhase next(RegionControllerPhase phase) {
        return static_cast<RegionControllerPhase>(
            (static_cast<size_t>(phase) + 1) % REGION_CONTROLLER_PHASE_COUNT
        );
    }

inline
    RegionControllerCensus::RegionControllerCensus()
        : count_(0)
        , min_cycle_(std::numeric_limits<Cycle>::max())
        , max_cycle_(std::numeric_limits<Cycle>::min())
    {
        for (size_t& counter: state_counts_) {
            counter = 0;
        }
        for (size_t& counter: phase_counts_) {
            counter = 0;
        }
        for (size_t& counter: action_counts_) {
            counter = 0;
        }
    }

inline
    RegionControllerCensus::RegionControllerCensus(const RegionControllerGroup& controllers)
        : RegionControllerCensus()
    {
        add(controllers);
    }

inline
    void RegionControllerCensus::add(const RegionController& controller) {
        count_ += 1;
        min_cycle_ = std::min(controller.cycle(), min_cycle_);
        max_cycle_ = std::max(controller.cycle(), max_cycle_);
        state_counts_[static_cast<size_t>(controller.state())] += 1;
        phase_counts_[static_cast<size_t>(controller.phase())] += 1;
        action_counts_[static_cast<size_t>(controller.action())] += 1;
    }

inline
    void RegionControllerCensus::add(const RegionControllerGroup& controllers) {
        for (const std::unique_ptr<RegionController>& controller: controllers) {
            add(*controller);
        }
    }

inline
    size_t RegionControllerCensus::count() const {
        return count_;
    }

inline
    auto RegionControllerCensus::min_cycle() const -> Cycle {
        return min_cycle_;
    }

inline
    auto RegionControllerCensus::max_cycle() const -> Cycle {
        return max_cycle_;
    }

inline
    bool RegionControllerCensus::any(State state) const {
        return state_counts_[static_cast<size_t>(state)] != 0;
    }

inline
    bool RegionControllerCensus::all(State state) const {
        return (count_ > 0) && state_counts_[static_cast<size_t>(state)] == count_;
    }

inline
    bool RegionControllerCensus::any(Phase phase) const {
        return phase_counts_[static_cast<size_t>(phase)] != 0;
    }

inline
    bool RegionControllerCensus::all(Phase phase) const {
        return (count_ > 0) && phase_counts_[static_cast<size_t>(phase)] == count_;
    }

inline
    bool RegionControllerCensus::any(Action action) const {
        return action_counts_[static_cast<size_t>(action)] != 0;
    }

inline
    bool RegionControllerCensus::all(Action action) const {
        return (count_ > 0) && action_counts_[static_cast<size_t>(action)] == count_;
    }

inline
    RegionController::RegionController(
        const RegionId region_id,
        RegionControllerGroup& controllers,
        WriteBarrierManager& write_barrier_manager
    )
        : region_id_(region_id)
        , controllers_(controllers)
        , write_barrier_manager_(write_barrier_manager)
        , state_(State::STARTING)
        , phase_(Phase::START)
        , cycle_(0)
        , write_barrier_(nullptr)
        , metrics_(operation_grouper_, object_grouper_)
    {
    }

inline
    RegionId RegionController::region_id() const {
        return region_id_;
    }

inline
    auto RegionController::metrics() const -> const Metrics& {
        return metrics_;
    }

inline
    bool RegionController::is_quiescent() const {
        return !operation_grouper_.is_dirty();
    }

inline
    auto RegionController::state() const -> State {
        return state_;
    }

inline
    auto RegionController::phase() const -> Phase {
        return phase_;
    }

inline
    auto RegionController::cycle() const -> Cycle {
        return cycle_;
    }

inline
    auto RegionController::action() const -> Action {
        return to_action(phase_);
    }

inline
    void RegionController::start(const Cycle cycle) {
        if (state_ != State::STARTING) {
            assert(false);
            return;
        }

        transition(cycle);
        transition(State::RUNNING);
    }

inline
    void RegionController::stop() {
        if (state_ != State::STOPPING) {
            assert(false);
            return;
        }

        transition(State::STOPPED);
    }

inline
    auto RegionController::send_message() -> std::optional<Message> {
        switch (phase_) {
            case Phase::START: {
                break;
            }
            case Phase::START_BARRIER: {
                break;
            }
            case Phase::ENTER: {
                transition(Phase::SUBMIT);

                return Message {
                    .enter = {
                        .type  = MessageType::ENTER,
                        .cycle = cycle_,
                    },
                };
            }
            case Phase::SUBMIT: {
                break;
            }
            case Phase::SUBMIT_BARRIER: {
                break;
            }
            case Phase::RETIRE_BARRIER: {
                break;
            }
            case Phase::RETIRE: {
                transition(Phase::LEAVE);

                return Message {
                    .retire = {
                        .type    = MessageType::RETIRE,
                        .garbage = object_grouper_.flush(),
                    },
                };
            }
            case Phase::LEAVE: {
                transition(Phase::START);
                if (state_ == State::STOPPED) {
                    transition(State::SHUTDOWN);
                }

                return Message {
                    .leave = {
                        .type = MessageType::LEAVE,
                        .stop = state_ == State::SHUTDOWN,
                    },
                };
            }
        }

        return std::nullopt;
    }

inline
    void RegionController::receive_message(const Message& message) {
        switch (phase_) {
            case Phase::START: {
                if (message.type == MessageType::START) {
                    transition(Phase::START_BARRIER);
                }
                break;
            }
            case Phase::START_BARRIER: {
                break; // Redundant start messages are dropped.
            }
            case Phase::ENTER: {
                break; // Redundant start messages are dropped.
            }
            case Phase::SUBMIT: {
                if (message.type == MessageType::SUBMIT) {
                    transition(Phase::SUBMIT_BARRIER);

                    if (message.submit.stop) {
                        if (state_ == State::STOPPED) {
                            // The region is reaffirming that wants to stop.
                        }
                        else {
                            transition(State::STOPPING);
                        }
                    }
                    else {
                        // The region is not quiescent anymore. We should
                        // stop shutting down until it is again.
                        transition(State::RUNNING);
                    }

                    // Hold onto this until all regions have submitted.
                    write_barrier_ = message.submit.write_barrier;
                }
                break; // Redundant start messages are dropped.
            }
            case Phase::SUBMIT_BARRIER: {
                break;
            }
            case Phase::RETIRE_BARRIER: {
                break;
            }
            case Phase::RETIRE: {
                break;
            }
            case Phase::LEAVE: {
                break;
            }
        }
    }

inline
    void RegionController::synchronize(const RegionControllerCensus& census) {
        Phase next_phase = next(phase_);
        Action next_action = to_action(next_phase);

        if (census.all(Action::BARRIER_ALL) || census.all(Action::BARRIER_ANY)) {
            // Sanity check that the cycle matches while synchronized.
            assert(census.min_cycle() == census.max_cycle());

            transition(next_phase);
        }
        else if (census.any(next_phase) && (next_action == Action::BARRIER_ANY)) {
            transition(next_phase);
        }
    }

inline
    void RegionController::transition(State next_state) {
        if (state_ == next_state) {
            return;
        }

        debug("[region_controller:{}] transition state {} to {}", region_id_, to_string(state_), to_string(next_state));
        state_ = next_state;
    }

inline
    void RegionController::transition(Phase next_phase) {
        if (phase_ == next_phase) {
            return;
        }

        switch (phase_) {
            case Phase::START: {
                // Some region asked the domain to start a coherence cycle.
                break;
            }
            case Phase::START_BARRIER: {
                // All controllers have started.
                break;
            }
            case Phase::ENTER: {
                // Ask the region to submit operations.
                break;
            }
            case Phase::SUBMIT: {
                // The region has submitted operations for us to route.
                break;
            }
            case Phase::SUBMIT_BARRIER: {
                // We are waiting for all regions to respond.
                while (WriteBarrierSegment* segment = write_barrier_->pop_back()) {
                    metrics_.increment_count += route_operations(OperationType::INCREMENT, segment->increment_records());
                    metrics_.decrement_count += route_operations(OperationType::DECREMENT, segment->decrement_records());

                    write_barrier_manager_.deallocate_segment(*segment);
                }

                // Make the write barrier ready for use again.
                write_barrier_->push_back(
                    write_barrier_manager_.allocate_segment()
                );
                break;
            }
            case Phase::RETIRE_BARRIER: {
                // All submitted operations have been routed. Flush and apply operations.
                const bool force = (state_ == State::STOPPING) || (state_ == State::STOPPED);
                operation_grouper_.flush(force);

                // Increments first to avoid premature death.
                for (auto&& [object, delta]: operation_grouper_.increments()) {
                    assert(delta >= 0);
                    const auto delta_magnitude = static_cast<uint32_t>(+delta);
                    if (!object->apply_increment(delta_magnitude)) {
                        abort();
                    }
                }

                // Apply decrements and group dead objects for finalization.
                for (auto&& [object, delta]: operation_grouper_.decrements()) {
                    assert(delta <= 0);
                    const auto delta_magnitude = static_cast<uint32_t>(-delta);
                    if (!object->apply_decrement(delta_magnitude)) {
                        object_grouper_.write(*object);
                    }
                }

                operation_grouper_.clear();
                break;
            }
            case Phase::RETIRE: {
                break;
            }
            case Phase::LEAVE: {
                transition(cycle_ + 1);
                break;
            }
        }

        debug("[region_controller:{}] transition phase {} to {}", region_id_, to_string(phase_), to_string(next_phase));
        phase_ = next_phase;
    }

inline
    void RegionController::transition(Cycle next_cycle) {
        if (cycle_ == next_cycle) {
            return;
        }

        debug("[region_controller:{}] transition cycle {} to {}", region_id_, cycle_, next_cycle);
        cycle_ = next_cycle;
    }

inline
    size_t RegionController::route_operations(const OperationType type, std::span<Object*> objects) {
        for (Object* object : objects) {
            if (!object) {
                continue; // Filter out operations on null pointers.
            }

            const RegionId region_id = object->region_id();
            if (UNLIKELY(region_id >= controllers_.size())) {
                abort();
            }

            RegionController& controller = *controllers_[region_id];
            controller.operation_grouper_.write(make_operation(object, type), false);
        }

        return objects.size();
    }

inline
    RegionControllerCensus synchronize(RegionControllerGroup& controllers) {
        RegionControllerCensus old_census;
        RegionControllerCensus new_census;

        // Synchronizing until we can no longer make forward progress without sending or receiving.
        do {
            old_census = RegionControllerCensus(controllers);

            for (auto&& controller: controllers) {
                controller->synchronize(old_census);
            }

            new_census = RegionControllerCensus(controllers);
        } while (old_census != new_census);

        return new_census;
    }

inline
    std::string_view to_string(RegionControllerState state) {
        using namespace std::literals;

        switch (state) {
#define X(MANTLE_REGION_CONTROLLER_STATE)                               \
            case RegionControllerState::MANTLE_REGION_CONTROLLER_STATE: \
                return #MANTLE_REGION_CONTROLLER_STATE ##sv;            \

        MANTLE_REGION_CONTROLLER_STATES(X)
#undef X
        }

        abort();
    }

inline
    std::string_view to_string(RegionControllerPhase phase) {
        using namespace std::literals;

        switch (phase) {
#define X(MANTLE_REGION_CONTROLLER_PHASE, MANTLE_REGION_CONTROLLER_ACTION) \
            case RegionControllerPhase::MANTLE_REGION_CONTROLLER_PHASE:    \
                return #MANTLE_REGION_CONTROLLER_PHASE ##sv;               \

        MANTLE_REGION_CONTROLLER_PHASES(X)
#undef X
        }

        abort();
    }

inline
    std::string_view to_string(RegionControllerAction action) {
        using namespace std::literals;

        switch (action) {
#define X(MANTLE_REGION_CONTROLLER_ACTION)                                \
            case RegionControllerAction::MANTLE_REGION_CONTROLLER_ACTION: \
                return #MANTLE_REGION_CONTROLLER_ACTION ##sv;             \

        MANTLE_REGION_CONTROLLER_ACTIONS(X)
#undef X
        }

        abort();
    }

}


// src/doorbell.cpp

namespace mantle {

inline
    Doorbell::Doorbell()
        : file_descriptor_(eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK))
    {
        if (file_descriptor_ < 0) {
            throw std::runtime_error("Failed to create doorbell eventfd");
        }
    }

inline
    Doorbell::~Doorbell() {
        close(file_descriptor_);
    }

inline
    int Doorbell::file_descriptor() {
        return file_descriptor_;
    }

inline
    void Doorbell::ring(uint64_t count) {
        ssize_t bytes_written = 0;
        do {
            bytes_written = write(file_descriptor_, &count, sizeof(count));
        } while ((bytes_written < 0) && (errno == EINTR));

        if (bytes_written != static_cast<ssize_t>(sizeof(count))) {
            abort();
        }
    }

inline
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


// src/mantle.cpp



// src/domain.cpp

namespace mantle {

inline
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

inline
    Domain::~Domain() {
        thread_.join();
    }

inline
    WriteBarrierManager& Domain::write_barrier_manager() {
        return write_barrier_manager_;
    }

inline
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

inline
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

inline
    void Domain::update_controllers(const RegionControllerCensus& census) {
        // Check if there are controllers that need to be started or stopped.
        // This is safe to do while there isn't an active cycle.
        if (controllers_.empty() || census.any(RegionControllerPhase::START)) {
            std::scoped_lock lock(regions_mutex_);

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

inline
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

inline
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

inline
    RegionId Domain::bind(Region& region) {
        std::scoped_lock lock(regions_mutex_);

        const RegionId region_id = regions_.size();
        regions_.push_back(&region);
        doorbell_.ring();

        return region_id;
    }

}


// src/selector.cpp

namespace mantle {

inline
    Selector::Selector()
         : epoll_fd_(epoll_create1(EPOLL_CLOEXEC))
    {
        if (epoll_fd_ < 0) {
            throw std::runtime_error("Failed to create epoll file descriptor");
        }
    }

inline
    Selector::~Selector() {
        int result = close(epoll_fd_);
        assert(result >= 0);
        epoll_fd_ = -1;
    }

inline
    std::span<void*> Selector::poll(bool non_blocking) {
        std::array<struct epoll_event, MAX_EVENT_COUNT> events;

        int event_count = 0;
        do {
            event_count = epoll_wait(epoll_fd_, events.data(), events.size(), non_blocking ? 0 : -1);
        } while ((event_count < 0) && (errno == EINTR));

        if (event_count < 0) {
            throw std::runtime_error("Failed to wait for epoll events");
        }

        for (int i = 0; i < event_count; ++i) {
            assert(events[i].events & EPOLLIN);

            poll_results_[i] = events[i].data.ptr;
        }

        return {
            poll_results_.data(),
            static_cast<size_t>(event_count)
        };
    }

inline
    void Selector::add_watch(int file_descriptor, void* user_data) {
        struct epoll_event event;
        memset(&event, 0, sizeof(event));
        event.events = EPOLLIN;
        event.data.ptr = user_data;

        int result = 0;
        do {
            result = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, file_descriptor, &event);
        } while ((result < 0) && (errno == EINTR));

        if (result < 0) {
            throw std::runtime_error("Failed to add epoll watch");
        }
    }

inline
    void Selector::modify_watch(int file_descriptor, void* user_data) {
        struct epoll_event event;
        memset(&event, 0, sizeof(event));
        event.events = EPOLLIN;
        event.data.ptr = user_data;

        int result = 0;
        do {
            result = epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, file_descriptor, &event);
        } while ((result < 0) && (errno == EINTR));

        if (result < 0) {
            throw std::runtime_error("Failed to modify epoll watch");
        }
    }

inline
    void Selector::delete_watch(int file_descriptor) {
        int result = 0;
        do {
            result = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, file_descriptor, nullptr);
        } while ((result < 0) && (errno == EINTR));

        if (result < 0) {
            throw std::runtime_error("Failed to delete epoll watch");
        }
    }

inline
    void wait_for_readable(int file_descriptor) {
        struct pollfd event;
        memset(&event, 0, sizeof(event));
        event.fd = file_descriptor;
        event.events = POLLIN;

        int result = 0;
        do {
            result = ::poll(&event, 1, -1);
        } while ((result < 0) && (errno == EINTR));

        if (result < 0) {
            throw std::runtime_error("Failed to poll file descriptor for readability");
        }

        assert(event.revents & POLLIN);
    }

}


// src/page_fault_handler.cpp

namespace mantle {

inline
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

inline
    PageFaultHandler::~PageFaultHandler() {
        const int result = close(file_descriptor_);
        assert(result >= 0);
    }

inline
    int PageFaultHandler::file_descriptor() const {
        return file_descriptor_;
    }

inline
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

inline
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

inline
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

inline
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

inline
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

inline
    uint64_t PageFaultHandler::translate(const std::initializer_list<Mode> modes) {
        uint64_t mask = 0;

        for (const Mode mode : modes) {
            mask |= translate(mode);
        }

        return mask;
    }

}


