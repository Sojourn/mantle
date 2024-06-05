#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <climits>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <emmintrin.h>
#include <fmt/core.h>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <poll.h>
#include <sched.h>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

// include/mantle/config.h


// TODO: Use macros for the global variables instead of global variables.

inline
inline
#endif

namespace mantle {

    // This flag enables weighted reference counting in handles.
    constexpr bool ENABLE_WEIGHTED_REFERENCE_COUNTING = false;
    constexpr bool ENABLE_OBJECT_GROUPING = true;

    // The number of messages that can be queued between `Domain` and `Region` endpoints.
    constexpr size_t STREAM_CAPACITY = 4096;


    // FIXME: Some architectures have cache lines that are 128 bytes. We should detect this.
    constexpr size_t CACHE_LINE_SIZE = 64;

    struct Config {
        std::optional<std::span<size_t>> domain_cpu_affinity;

        // The maximum number of pending operations per-region.
        size_t ledger_capacity = 1024 * 1024;

        // This enables the grouper which tries to consolidate operations on the same object.
        // and net their effects to reduce the number of operations that need to be retired/applied.
        bool operation_grouper_enabled = true;
    };
}


// include/mantle/types.h


namespace mantle {

    class Object;

    using RegionId        = uint16_t;
    using ObjectGroup     = uint16_t;
    using ObjectGroupMask = std::array<uint64_t, std::numeric_limits<ObjectGroup>::max() / (sizeof(uint64_t) * CHAR_BIT)>;
    using AtomicSequence  = std::atomic_uint64_t;
    using Sequence        = AtomicSequence::value_type;

    // TODO: Make `Message` use a variant instead of a union so we can construct this properly.
    struct SequenceRange {
        Sequence head;
        Sequence tail;

        [[nodiscard]]
        constexpr size_t size() const {
            return tail - head;
        }

        constexpr auto operator<=>(const SequenceRange&) const noexcept = default;
    };

    constexpr SequenceRange EMPTY_SEQUENCE_RANGE = { .head=0, .tail=0 };

    // TODO: Move this into a separate file. It knows too much aabout other classes.
    struct ObjectGroups {
        Object**         objects;
        size_t           object_count;
        ObjectGroup      group_min;     // Inclusive.
        ObjectGroup      group_max;     // Inclusive.
        size_t*          group_offsets; // Offsets into the objects array (where to find members).
        ObjectGroupMask* group_mask;    // A bitset of non-empty groups.

        [[nodiscard]]
        size_t group_member_count(ObjectGroup group) const {
            if constexpr (!ENABLE_OBJECT_GROUPING) {
                abort();
            }

            return group_offsets[static_cast<size_t>(group) + 1] - group_offsets[group];
        }

        [[nodiscard]]
        std::span<Object*> group_members(ObjectGroup group) {
            if constexpr (!ENABLE_OBJECT_GROUPING) {
                abort();
            }

            return {
                &objects[group],
                group_member_count(group)
            };
        }

        template<typename Visitor>
        void for_each_group(Visitor&& visitor) {
            if constexpr (!ENABLE_OBJECT_GROUPING) {
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
#else
#  define MANTLE_HOT inline
#endif

#define MANTLE_COLD

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
            throw std::runtime_error(fmt::format("Failed to set cpu affinity - {}", strerror(errno)));
        }

        std::this_thread::yield();
    }

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
        // This is also a useful optimization for weighted references which are usually
        // split on powers-of-two.
        static constexpr uintptr_t EXPONENT_BITS  = 3;
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

    // NOTE: `capacity == size` since it is always padded by null operations.
    struct alignas(64) OperationBatch {
        static constexpr size_t SIZE  = CACHE_LINE_SIZE / sizeof(Operation);
        static constexpr size_t SHIFT = log2_floor(SIZE);
        static constexpr size_t MASK  = SIZE - 1; // TODO: Remove this.

        Operation operations[SIZE];

        Operation& operator[](Sequence sequence);
        const Operation& operator[](Sequence sequence) const;
    };

    struct OperationRange {
        OperationBatch* head;
        OperationBatch* tail;
    };

    inline Operation& OperationBatch::operator[](const Sequence sequence) {
        return operations[sequence % SIZE];
    }

    inline const Operation& OperationBatch::operator[](const Sequence sequence) const {
        return operations[sequence % SIZE];
    }

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

    template<typename OperationHandler>
    void for_each_operation(const OperationBatch* first, const OperationBatch* last, OperationHandler&& handler) {
        for (const OperationBatch* batch = first; batch != last; ++batch) {
            for (const Operation& operation: batch->operations) {
                handler(operation);
            }
        }
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


// include/mantle/object.h


namespace mantle {

    // This alignment gives us 4 tag bits to use in the encoding of an operation.
    class alignas(16) Object {
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
        friend class Handle;
        friend class Region;
        friend class RegionController;

        // Associate this `Object` to the local `Region`. Reference counting
        // and object finalization will be handled by that `Region. An `Object`
        // can only be bound once, when a handle to it is first created.
        //
        void bind(RegionId region_id);

        // Submit an operation to the `Region` who will forward it to the `Domain`.
        void start_increment_operation(uint8_t exponent);
        void start_increment_operation(Operation operation);
        void start_decrement_operation(uint8_t exponent);
        void start_decrement_operation(Operation operation);

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


// include/mantle/operation_writer.h


namespace mantle {

    // This class streams batches of operations to memory, bypassing the CPU cache hierarchy.
    template<typename Storage_>
    class OperationWriter {
    public:
        using Storage = Storage_;

        OperationWriter(Storage& storage, Sequence head = 0, Sequence tail = 0)
            : storage_(storage)
            , head_(0)
            , tail_(0)
        {
            memset(&batch_, 0, sizeof(batch_));

            reset(head, tail);
        }

        OperationWriter(OperationWriter&&) = delete;
        OperationWriter(const OperationWriter&) = delete;
        OperationWriter& operator=(OperationWriter&&) = delete;
        OperationWriter& operator=(const OperationWriter&) = delete;

        Sequence tell() const {
            return head_;
        }

        // TODO: Look at the code-gen for this. I think it might suck.
        //       We want a fairly short instruction sequence so it can inline.
        MANTLE_HOT bool write(Operation operation) {
            if (head_ == tail_) {
                return false;
            }

            size_t operation_index = head_ & OperationBatch::MASK;
            batch_.operations[operation_index] = operation;

            // Stream the batch to memory if we just completed it.
            if (operation_index == OperationBatch::MASK) {
                size_t batch_index = head_ >> OperationBatch::SHIFT;

                OperationBatch* target_batch = &storage_[batch_index];
                __m128i* target_pointer = (__m128i*)target_batch;

                const OperationBatch* source_batch = &batch_;
                const __m128i* source_pointer = (const __m128i*)source_batch;

                _mm_stream_si128(target_pointer+0, _mm_load_si128(source_pointer+0));
                _mm_stream_si128(target_pointer+1, _mm_load_si128(source_pointer+1));
                _mm_stream_si128(target_pointer+2, _mm_load_si128(source_pointer+2));
                _mm_stream_si128(target_pointer+3, _mm_load_si128(source_pointer+3));
            }

            head_ += 1;

            return true;
        }

        // Pad the current batch with null operations and write it out if it is partially full.
        //
        // !!! This must be called to make prior writes visible in other threads. !!!
        //
        void flush() {
            while (head_ & OperationBatch::MASK) {
                write(make_null_operation());
            }

            _mm_sfence();
        }

        void reset(Sequence head = 0, Sequence tail = 0) {
            assert(!(head & OperationBatch::MASK));
            assert(!(tail & OperationBatch::MASK));

            head_ = head;
            tail_ = tail;
        }

    private:
        Storage&       storage_;
        Sequence       head_;
        Sequence       tail_;
        OperationBatch batch_;
    };

    using OperationVector = std::vector<OperationBatch>;

    class OperationVectorWriter : private OperationWriter<OperationVector> {
    public:
        using Base = OperationWriter<OperationVector>;

        OperationVectorWriter(size_t capacity = 0)
            : Base(storage_)
        {
            storage_.reserve(capacity);
        }

        OperationRange data() {
            return {
                .head = storage_.data(),
                .tail = storage_.data() + storage_.size(),
            };
        }

        std::span<OperationBatch> span() {
            return {
                storage_.data(),
                storage_.size(),
            };
        }

        void write(Operation operation) {
            // Fast-path: the current batch has space for this operation.
            if (LIKELY(Base::write(operation))) {
                return;
            }

            // Append a new, empty batch.
            {
                Sequence new_head = Base::tell();
                Sequence new_tail = new_head + OperationBatch::SIZE;

                OperationBatch batch;
                memset(&batch, 0, sizeof(batch));
                storage_.push_back(batch);

                Base::reset(new_head, new_tail);
            }

            // This write will succeed with the additional capacity.
            bool written = Base::write(operation);
            (void)written;
            assert(written); // TODO: Make a `MANTLE_ASSERT` macro that does a void cast in release builds.
        }

        void flush() {
            Base::flush();
        }

        void clear() {
            storage_.clear();
            Base::reset(0, 0);
        }

        std::vector<OperationBatch> release() {
            return std::move(storage_);
        }

    private:
        std::vector<OperationBatch> storage_;
    };

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
            SequenceRange increments;
            SequenceRange decrements;
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

    constexpr Message make_enter_message(Sequence cycle) {
        return {
            .enter = {
                .type  = MessageType::ENTER,
                .cycle = cycle,
            }
        };
    }

    constexpr Message make_leave_message(bool stop) {
        return {
            .leave = {
                .type = MessageType::LEAVE,
                .stop = stop,
            }
        };
    }

    constexpr std::string_view to_string(MessageType type) {
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


// include/mantle/operation_ledger.h


namespace mantle {

    class SequenceRangeHistory {
    public:
        explicit SequenceRangeHistory(size_t capacity)
            : next_slot_(0)
            , data_(capacity)
        {
            data_.fill(0);
        }

        [[nodiscard]]
        size_t capacity() const {
            return data_.size();
        }

        [[nodiscard]]
        SequenceRange select(int age = 0) const {
            const Sequence prev_slot = next_slot_ - 1;

            return {
                .head = data_[prev_slot - age - 1],
                .tail = data_[prev_slot - age - 0],
            };
        }

        // NOTE: The `head` of the new range is implicitly the `tail` of the
        //       previously inserted `SequenceRange`.
        void insert(const Sequence tail) {
            data_[next_slot_++] = tail;
        }

    private:
        Sequence       next_slot_;
        Ring<Sequence> data_;
    };

    class OperationLedger {
    public:
        explicit OperationLedger(size_t ledger_capacity)
            : storage_(ledger_capacity)
            , transaction_log_(TRANSACTION_LOG_HISTORY)
            , transaction_head_(0)
            , transaction_tail_(storage_.size())
            , writer_(storage_, transaction_head_, transaction_tail_)
        {
        }

        [[nodiscard]]
        const SequenceRangeHistory& transaction_log() const {
            return transaction_log_;
        }

        [[nodiscard]]
        bool is_empty() const {
            return (transaction_tail_ - writer_.tell()) == storage_.size();
        }

        void begin_transaction() {
            transaction_head_ = writer_.tell();
            transaction_tail_ = transaction_log_.select(-1).tail + storage_.size();

            writer_.reset(transaction_head_, transaction_tail_);
        }

        SequenceRange commit_transaction() {
            writer_.flush();
            transaction_log_.insert(writer_.tell());
            return transaction_log_.select(0);
        }

        // Returns a reference to the batch containing this operation sequence.
        //
        // NOTE: Reading an operation batch that hasn't been published in a transaction is undefined behavior.
        //
        [[nodiscard]]
        const OperationBatch& read_batch(const Sequence sequence) const {
            return storage_[sequence >> OperationBatch::SHIFT];
        }

        // Returns a reference to the operation corresponding to this sequence.
        //
        // NOTE: Reading an operation that hasn't been published in a transaction is undefined behavior.
        //
        [[nodiscard]]
        const Operation& read(const Sequence sequence) const {
            return read_batch(sequence).operations[sequence & OperationBatch::MASK];
        }

        // Adds an operation to the current, uncommitted transaction.
        // This can fail and return false if the ledger is full.
        MANTLE_HOT bool write(const Operation operation) {
            return writer_.write(operation);
        }

        // Return the number of entries that can still be written to the current transaction.
        [[nodiscard]]
        size_t writable_transaction_entries() const {
            const Sequence ceiling = transaction_log_.select(-1).tail + storage_.size();
            return ceiling - writer_.tell();
        }

    private:
        static constexpr size_t TRANSACTION_LOG_HISTORY = 4;

        using Storage = Ring<OperationBatch>;
        using Writer = OperationWriter<Storage>;

        Storage               storage_;
        SequenceRangeHistory  transaction_log_;
        Sequence              transaction_head_;
        Sequence              transaction_tail_;
        Writer                writer_;
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

            if constexpr (ENABLE_OBJECT_GROUPING) {
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
        Stream(size_t minimum_capacity = STREAM_CAPACITY)
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
        struct alignas(CACHE_LINE_SIZE) Slot {
            Message message;
        };

        std::vector<Slot> ring_;
        size_t            mask_;

        alignas(CACHE_LINE_SIZE) AtomicSequence head_;
        alignas(CACHE_LINE_SIZE) AtomicSequence tail_;

        alignas(CACHE_LINE_SIZE) Sequence private_head_; // Private to receive.
        alignas(CACHE_LINE_SIZE) Sequence private_tail_; // Private to send.
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
            const OperationLedger& ledger,
            const Config& config
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

        size_t route_operations(OperationType type, SequenceRange range);

    private:
        RegionId               region_id_;
        RegionControllerGroup& controllers_;
        const OperationLedger& ledger_;
        const Config&          config_;

        State                  state_;
        Phase                  phase_;
        Cycle                  cycle_;

    SequenceRange          submitted_increments_;
        SequenceRange          submitted_decrements_;

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
    class ObjectFinalizer;

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

        Region(Domain& domain, ObjectFinalizer& finalizer);
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
        template<typename T>
        friend class Handle;
        friend class Object;

        MANTLE_HOT void start_increment_operation(Object& object, Operation operation);
        MANTLE_HOT void start_decrement_operation(Object& object, Operation operation);

        MANTLE_COLD void flush_operation(Operation operation);

    private:
        friend class Domain;

        const OperationLedger& ledger() const;

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

        ObjectFinalizer&            finalizer_;
        OperationLedger             ledger_;

        std::optional<ObjectGroups> garbage_;
        std::vector<Object*>        garbage_pile_;

        Connection                  connection_;
    };

    inline void Region::start_increment_operation(Object&, Operation operation) {
        assert(state_ != State::STOPPED);
        assert(operation.type() == OperationType::INCREMENT);

        // Fast-path: The operation can be added to the current transaction.
        if (LIKELY(ledger_.write(operation))) {
            return;
        }

        flush_operation(operation);
    }

    inline void Region::start_decrement_operation(Object&, Operation operation) {
        assert(state_ != State::STOPPED);
        assert(operation.type() == OperationType::DECREMENT);

        // Fast-path: The operation can be added to the current transaction.
        if (LIKELY(ledger_.write(operation))) {
            return;
        }

        flush_operation(operation);
    }

    std::string_view to_string(RegionState state);
    std::string_view to_string(RegionPhase phase);

    bool has_region();
    Region& get_region();

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


// include/mantle/domain.h


namespace mantle {

    class Region;

    class Domain {
        friend class Region;

    public:
        explicit Domain(const Config& config = Config());
        ~Domain();

        Domain(Domain&&) = delete;
        Domain(const Domain&) = delete;
        Domain& operator=(Domain&&) = delete;
        Domain& operator=(const Domain&) = delete;

        [[nodiscard]]
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


// include/mantle/handle.h


namespace mantle {

    // This class holds a strong reference to an Object derived class instance.
    // It implements a smart-pointer like interface and has semantics similar to std::shared_ptr.
    //
    // NOTE: We've got an extra `OperationType` bit we can use for a flag if needed.
    // NOTE: The exponent is never saturated at rest. We can use the high bit for a flag if needed.
    // TODO: Think about renaming this to `Ref<T>` for brevity.
    //
    template<typename T>
    class Handle {
        static_assert(std::is_base_of_v<Object, T>, "Object is a required base class");

        friend Handle<T> make_handle<T>(T& object) noexcept;

        // Bind an `Object` subclass to the local `Region` and return a managed `Handle` to it.
        static Handle bind(T& object) noexcept {
            static_cast<Object&>(object).bind(get_region().id());

            return Handle(make_decrement_operation(&object, Operation::EXPONENT_MIN));
        }

        explicit Handle(Operation operation) noexcept
            : operation_(operation)
        {
        }

    public:
        Handle() noexcept
            : operation_(make_null_operation())
        {
        }

        Handle(std::nullptr_t) noexcept
            : Handle()
        {
        }

        Handle(Handle&& other) noexcept
            : operation_(make_null_operation())
        {
            std::swap(operation_, other.operation_);
        }

        template<typename U>
        Handle(Handle<U>&& other) noexcept
            : operation_(make_null_operation())
        {
            std::swap(operation_, other.operation_);
        }

        Handle(const Handle& other) noexcept
            : Handle(other.copy_reference())
        {
        }

        template<typename U>
        Handle(const Handle<U>& other) noexcept
            : operation_(other.copy_reference())
        {
            static_assert(std::is_base_of_v<T, U>);
        }

        Handle& operator=(Handle&& that) noexcept {
            if (operation_.object() != that.operation_.object()) {
                reset();
                std::swap(operation_, that.operation_);
            }

            return *this;
        }

        template<typename U>
        Handle& operator=(Handle<U>&& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            if (operation_.object() != that.operation_.object()) {
                reset();
                std::swap(operation_, that.operation_);
            }

            return *this;
        }

        Handle& operator=(const Handle& that) noexcept {
            if (operation_.object() != that.operation_.object()) {
                reset();
                operation_ = that.copy_reference();
            }

            return *this;
        }

        template<typename U>
        Handle& operator=(const Handle<U>& that) noexcept {
            static_assert(std::is_base_of_v<T, U>);

            if (operation_.object() != that.operation_.object()) {
                reset();
                operation_ = that.copy_reference();
            }

            return *this;
        }

        ~Handle() noexcept {
            reset();
        }

        T* get() noexcept {
            return static_cast<T*>(operation_.mutable_object());
        }

        const T* get() const noexcept {
            return static_cast<const T*>(operation_.object());
        }

        T* operator->() noexcept {
            assert(*this);

            return get();
        }

        const T* operator->() const noexcept {
            assert(*this);

            return get();
        }

        T& operator*() noexcept {
            assert(*this);

            return *get();
        }

        const T& operator*() const noexcept {
            assert(*this);

            return *get();
        }

        explicit operator bool() const noexcept {
            return static_cast<bool>(operation_);
        }

        void reset() noexcept {
            if (operation_) {
                Object* object = operation_.mutable_object();
                assert(object);
                object->start_decrement_operation(operation_);

                operation_ = make_null_operation();
            }

            assert(!operation_);
        }

    public:
        uint8_t weight() const {
            return operation_.exponent();
        }

    private:
        [[nodiscard]] MANTLE_HOT Operation copy_reference() const {
            Object* object = operation_.mutable_object();
            if (!object) {
                return make_null_operation();
            }

            if constexpr (ENABLE_WEIGHTED_REFERENCE_COUNTING) {
                // Check if we need to gain additional weight.
                if (weight() == 0) {
                    // NOTE: Submit the new operation before the old operation.
                    object->start_increment_operation(make_increment_operation(object, Operation::EXPONENT_MAX));
                    object->start_decrement_operation(operation_);
                    operation_ = make_decrement_operation(object, Operation::EXPONENT_MAX);
                }

                // Split our weight in half by reducing the exponent by one.
                operation_ = make_decrement_operation(object, weight() - 1);
                return operation_;
            }
            else {
                // Create a paired increment and decrement.
                Operation increment = make_increment_operation(object);
                Operation decrement = make_decrement_operation(object);

                // The increment can be started immediately.
                object->start_increment_operation(increment);

                // The decrement will be started once the new reference is dropped.
                return decrement;
            }
        }

    private:
        mutable Operation operation_;
    };

    template<typename T>
    inline Handle<T> make_handle(T& object) noexcept {
        return Handle<T>::bind(object);
    }

}


// include/mantle/object_finalizer.h


namespace mantle {

    // An interface for cleaning up objects once they are no longer referenced.
    class ObjectFinalizer {
    public:
        virtual ~ObjectFinalizer() = default;

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

        stream << fmt::format(
            "Operation(object:{}, value:{})"
            , static_cast<const void*>(mutable_operation.object())
            , static_cast<int>(mutable_operation.value())
        );

        return stream;
    }

    inline std::ostream& operator<<(std::ostream& stream, const OperationBatch& batch) {
        stream << "OperationBatch(\n";
        for (Operation operation: batch.operations) {
            if (operation) {
                stream << "  " << operation << ",\n";
            }
        }
        stream << ')';

        return stream;
    }

    inline std::ostream& operator<<(std::ostream& stream, const RegionControllerGroup& controllers) {
        stream << "RegionControllerGroup(\n";
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            auto&& controller = *controllers[region_id];

            stream << fmt::format(
                "  RegionController(id:{}, phase:{}, action:{})",
                region_id,
                to_string(controller.phase()),
                to_string(controller.action())
            ) << '\n';
        }

        stream << ')';

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
    Region::Region(Domain& domain, ObjectFinalizer& finalizer)
        : domain_(domain)
        , id_(std::numeric_limits<RegionId>::max())
        , state_(INITIAL_STATE)
        , phase_(INITIAL_PHASE)
        , cycle_(INITIAL_CYCLE)
        , depth_(0)
        , finalizer_(finalizer)
        , ledger_(domain.config().ledger_capacity)
    {
        // Register ourselves as the region on this thread.
        {
            if (region_instance) {
                throw std::runtime_error("Cannot have more than one region per thread");
            }

            region_instance = this;
        }

        // Synchronize with other regions until our cycle and phase match.
        {
            ledger_.begin_transaction();

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

        assert(region_instance == this);
        region_instance = nullptr;
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
    void Region::flush_operation(Operation operation) {
        do {
            constexpr bool non_blocking = false;
            step(non_blocking);
        } while (!ledger_.write(operation));
    }

inline
    const OperationLedger& Region::ledger() const {
        return ledger_;
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

                // Wrap up the current transaction and submit ranges of operations
                // that can be applied.
                ledger_.commit_transaction();
                {
                    // Check if the region is ready to stop.
                    bool stop = true;
                    stop &= state_ == State::STOPPING;
                    stop &= ledger_.is_empty();

                    region_endpoint().send_message(
                        Message {
                            .submit = {
                                .type       = MessageType::SUBMIT,
                                .stop       = stop,
                                .increments = ledger_.transaction_log().select(0),
                                .decrements = ledger_.transaction_log().select(2),
                            },
                        }
                    );
                }
                ledger_.begin_transaction();

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
        if (depth_) {
            // `Region::step` and `ObjectFinalizer::finalize` are co-recursive.
            // Short circuiting object finalization in nested `Region::step` calls
            // prevents unbounded stack usage.
            assert(depth_ == 1);

            if (garbage_) {
                // Add garbage to the pile until we can safely deal with it.
                garbage_->for_each_group([this](ObjectGroup, std::span<Object*> members) {
                    garbage_pile_.insert(
                        garbage_pile_.end(),
                        members.begin(),
                        members.end()
                    );
                });

                garbage_.reset();
            }
        }
        else {
            ScopedIncrement lock(depth_);

            if (garbage_) {
                if constexpr (ENABLE_OBJECT_GROUPING) {
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

            if (UNLIKELY(!garbage_pile_.empty())) {
                // This collection can be modified while we are iterating over it.
                // Use index-based iteration to avoid invalidation issues.
                for (size_t i = 0; i < garbage_pile_.size(); ++i) {
                    Object* object = garbage_pile_[i];
                    finalizer_.finalize(object->group(), std::span{&object, 1});
                }

                garbage_pile_.clear();
            }
        }
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

inline
    bool has_region() {
        return region_instance != nullptr;
    }

inline
    Region& get_region() {
        assert(has_region());

        return *region_instance;
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
                increments_.emplace_back(operation.mutable_object(), operation.value());
            }
            else {
                decrements_.emplace_back(operation.mutable_object(), operation.value());
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
    void Object::start_increment_operation(uint8_t exponent) {
        start_increment_operation(make_increment_operation(this, exponent));
    }

inline
    void Object::start_increment_operation(Operation operation) {
        assert(operation.type() == OperationType::INCREMENT);

        info("[object:{}] start increment - exponent:{}", (const void*)this, operation.exponent());

        if (LIKELY(has_region())) {
            get_region().start_increment_operation(*this, operation);
        }
        else {
#if MANTLE_AUDIT
            assert(false); // Leak?
#endif
        }
    }

inline
    void Object::start_decrement_operation(uint8_t exponent) {
        start_decrement_operation(make_decrement_operation(this, exponent));
    }

inline
    void Object::start_decrement_operation(Operation operation) {
        assert(operation.type() == OperationType::DECREMENT);

        info("[object:{}] start decrement - exponent:{}", (const void*)this, operation.exponent());

        if (LIKELY(has_region())) {
            get_region().start_decrement_operation(*this, operation);
        }
        else {
#if MANTLE_AUDIT
            assert(false); // Leak?
#endif
        }
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
        const OperationLedger& ledger,
        const Config& config
    )
        : region_id_(region_id)
        , controllers_(controllers)
        , ledger_(ledger)
        , config_(config)
        , state_(State::STARTING)
        , phase_(Phase::START)
        , cycle_(0)
        , submitted_increments_(EMPTY_SEQUENCE_RANGE)
        , submitted_decrements_(EMPTY_SEQUENCE_RANGE)
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

                    submitted_increments_ = message.submit.increments;
                    submitted_decrements_ = message.submit.decrements;
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
                // NOTE: This phase can be combined with the next if we double buffer retired operations.
                metrics_.increment_count += route_operations(
                    OperationType::INCREMENT,
                    submitted_increments_
                );
                metrics_.decrement_count += route_operations(
                    OperationType::DECREMENT,
                    submitted_decrements_
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
    size_t RegionController::route_operations(const OperationType type, SequenceRange range) {
        size_t count = 0;

        for (Sequence sequence = range.head; sequence != range.tail; ++sequence) {
            Operation operation = ledger_.read(sequence);
            if (type != operation.type()) {
                continue;
            }

            const Object* object = operation.object();
            if (!object) {
                continue;
            }

            const RegionId region_id = object->region_id();
            if (UNLIKELY(region_id >= controllers_.size())) {
                abort();
            }

            RegionController& controller = *controllers_[region_id];
            controller.operation_grouper_.write(operation, true);

            count += 1;
        }

        return count;
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
            throw std::runtime_error(fmt::format("Failed to create doorbell eventfd - {}", strerror(errno)));
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
    Domain::Domain(const Config& config)
        : config_(config)
        , running_(false)
    {
        selector_.add_watch(doorbell_.file_descriptor(), &doorbell_);

        std::promise<void> init_promise;
        std::future<void> init_future = init_promise.get_future();

        thread_ = std::thread([init_promise = std::move(init_promise), this]() mutable {
            try {
                debug("[domain] initializing thread");
                if (config_.domain_cpu_affinity) {
                    set_cpu_affinity(*config_.domain_cpu_affinity);
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
    const Config& Domain::config() const {
        return config_;
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

        if (user_data == &doorbell_) {
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
                auto controller = std::make_unique<RegionController>(region_id, controllers_, region.ledger(), config_);
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


