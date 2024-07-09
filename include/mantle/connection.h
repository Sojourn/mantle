#pragma once

#include <new>
#include <span>
#include <array>
#include <vector>
#include <mutex>
#include <type_traits>
#include <cstdint>
#include <cstddef>
#include <cassert>
#include <sys/eventfd.h>
#include "mantle/util.h"
#include "mantle/ring.h"
#include "mantle/types.h"
#include "mantle/config.h"
#include "mantle/message.h"
#include "mantle/doorbell.h"

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
