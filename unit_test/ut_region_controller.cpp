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

static void deliver_submit_message(RegionControllerGroup& controllers, RegionId region_id, SequenceRange increments, SequenceRange decrements) {
    Message message = {
        .submit = {
            .type          = MessageType::SUBMIT,
            .stop          = false,
            .increments    = increments,
            .decrements    = decrements,
            .write_barrier = nullptr,
        },
    };

    deliver_message(controllers, region_id, message);
}

TEST_CASE("RegionController") {
    using Phase = RegionControllerPhase;
    using Action = RegionControllerAction;

    Config config;

    WriteBarrierManager write_barrier_manager;

    std::vector<std::unique_ptr<OperationLedger>> ledgers;
    ledgers.push_back(std::make_unique<OperationLedger>(config.ledger_capacity));
    ledgers.push_back(std::make_unique<OperationLedger>(config.ledger_capacity));
    ledgers.push_back(std::make_unique<OperationLedger>(config.ledger_capacity));
    ledgers.push_back(std::make_unique<OperationLedger>(config.ledger_capacity));

    RegionControllerGroup controllers;
    for (RegionId region_id = 0; region_id < ledgers.size(); ++region_id) {
        controllers.push_back(
            std::make_unique<RegionController>(
                region_id,
                controllers,
                *ledgers[region_id],
                write_barrier_manager,
                config
            )
        );
    }

    RegionControllerCensus census = RegionControllerCensus(controllers);

    SequenceRange empty_sequence_range = {0, 0};
    const SequenceRange& increments = empty_sequence_range;
    const SequenceRange& decrements = empty_sequence_range;

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

        deliver_submit_message(controllers, 0, increments, decrements);
        census = synchronize(controllers);
        CHECK(census.any(Phase::SUBMIT));
        CHECK(census.any(Phase::SUBMIT_BARRIER));

        for (RegionId region_id = 0; region_id < controllers.size(); ++region_id) {
            deliver_submit_message(controllers, region_id, increments, decrements);
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
