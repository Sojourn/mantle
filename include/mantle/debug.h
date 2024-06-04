#pragma once

#include <iostream>
#include <string>
#include <fmt/core.h>
#include "mantle/types.h"
#include "mantle/operation.h"
#include "mantle/domain.h"
#include "mantle/region.h"
#include "mantle/region_controller.h"

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
