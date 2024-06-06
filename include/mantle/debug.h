#pragma once

#include <string>
#include <ostream>
#include <sstream>
#include "mantle/types.h"
#include "mantle/operation.h"
#include "mantle/domain.h"
#include "mantle/region_controller.h"

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

    inline std::ostream& operator<<(std::ostream& stream, const OperationBatch& batch) {
        std::stringstream ss;
        ss << "OperationBatch(\n";
        ss << "  operations: [";
        for (Operation operation: batch.operations) {
            if (operation) {
                ss << operation << ", ";
            }
        }
        ss << "]";

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
