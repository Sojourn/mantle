#pragma once

#include <span>
#include <array>

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
