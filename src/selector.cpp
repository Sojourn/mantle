#include "mantle/selector.h"
#include <stdexcept>
#include <cstring>
#include <cassert>

#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>

namespace mantle {

    Selector::Selector()
         : epoll_fd_(epoll_create1(EPOLL_CLOEXEC))
    {
        if (epoll_fd_ < 0) {
            throw std::runtime_error("Failed to create epoll file descriptor");
        }
    }

    Selector::~Selector() {
        int result = close(epoll_fd_);
        assert(result >= 0);
        epoll_fd_ = -1;
    }

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

    void Selector::delete_watch(int file_descriptor) {
        int result = 0;
        do {
            result = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, file_descriptor, nullptr);
        } while ((result < 0) && (errno == EINTR));

        if (result < 0) {
            throw std::runtime_error("Failed to delete epoll watch");
        }
    }

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
