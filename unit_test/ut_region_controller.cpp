#include "catch.hpp"
#include "mantle/ledger.h"
#include "mantle/object.h"
#include "mantle/region_controller.h"

using namespace mantle;

static void deliver_message(RegionControllerGroup& controllers, RegionId region_id, const Message& message) {
    controllers.at(region_id)->receive_message(message);
}

static void deliver_start_message(RegionControllerGroup& controllers, RegionId region_id) {
    Message message = {
        .start = {
            .type = MessageType::START,
        },
    };

    deliver_message(controllers, region_id, message);
}

static void deliver_submit_message(RegionControllerGroup& controllers, RegionId region_id, WriteBarrier& write_barrier) {
    Message message = {
        .submit = {
            .type          = MessageType::SUBMIT,
            .stop          = false,
            .write_barrier = &write_barrier,
        },
    };

    deliver_message(controllers, region_id, message);
}

TEST_CASE("RegionController") {
    using Phase = RegionControllerPhase;
    using Action = RegionControllerAction;

    WriteBarrierManager write_barrier_manager;
    Ledger ledger(write_barrier_manager);

    RegionControllerGroup controllers;
    for (RegionId region_id = 0; region_id < 4; ++region_id) {
        controllers.push_back(
            std::make_unique<RegionController>(
                region_id,
                controllers,
                write_barrier_manager
            )
        );
    }

    RegionControllerCensus census = RegionControllerCensus(controllers);

    SECTION("Start initiated by one region") {
        deliver_start_message(controllers, 0);

        census = synchronize(controllers);
        CHECK(census.all(Phase::ENTER));
    }

    SECTION("Start initiated by all regions") {
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            deliver_start_message(controllers, region_id);
        }

        census = synchronize(controllers);
        CHECK(census.all(Phase::ENTER));
    }

    SECTION("Empty cycle") {
        // Start initiated by the first region.
        deliver_start_message(controllers, 0);
        census = RegionControllerCensus(controllers);
        CHECK(census.any(Action::BARRIER_ANY));

        // Synchronize to advance past the start barrier.
        census = synchronize(controllers);
        CHECK(census.all(Action::SEND));
        CHECK(census.all(Phase::ENTER));

        // Controllers should send an `ENTER` message to begin the cycle.
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            std::optional<Message> message = controllers[region_id]->send_message();

            REQUIRE(message);
            CHECK(message->type == MessageType::ENTER);
        }

        // Controllers should be waiting to receive a submit message from controllers.
        census = RegionControllerCensus(controllers);
        CHECK(census.all(Action::RECEIVE));
        CHECK(census.all(Phase::SUBMIT));

        ledger.commit();
        deliver_submit_message(controllers, 0, ledger.barrier(WriteBarrierPhase::APPLY));
        census = synchronize(controllers);
        CHECK(census.any(Phase::SUBMIT));
        CHECK(census.any(Phase::SUBMIT_BARRIER));

        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            ledger.commit();
            deliver_submit_message(controllers, region_id, ledger.barrier(WriteBarrierPhase::APPLY));
        }
        census = RegionControllerCensus(controllers);
        CHECK(census.all(Phase::SUBMIT_BARRIER));

        // Controllers should be ready to send retire messages.
        // NOTE: This will also advance past `RETIRE_BARRIER`.
        census = synchronize(controllers);
        CHECK(census.all(Phase::RETIRE));

        // Check that synchronizing now is a no-op.
        census = synchronize(controllers);
        CHECK(census.all(Phase::RETIRE));

        // Broadcast retire messages.
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            std::optional<Message> message = controllers[region_id]->send_message();

            REQUIRE(message);
            CHECK(message->type == MessageType::RETIRE);
        }

        // Controllers should be ready to send leave messages.
        census = RegionControllerCensus(controllers);
        CHECK(census.all(Phase::LEAVE));

        // Check that synchronizing now is a no-op.
        census = synchronize(controllers);
        CHECK(census.all(Phase::LEAVE));

        // Broadcast leave messages.
        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            std::optional<Message> message = controllers[region_id]->send_message();

            REQUIRE(message);
            CHECK(message->type == MessageType::LEAVE);
        }

        // Controllers should have returned to the starting Phase.
        census = RegionControllerCensus(controllers);
        CHECK(census.all(Phase::START));
    }
}
