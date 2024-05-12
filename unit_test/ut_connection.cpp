#include "catch.hpp"
#include "mantle/connection.h"

using namespace mantle;

TEST_CASE("Connection") {
    Connection connection;
    Endpoint& client_endpoint = connection.client_endpoint();
    Endpoint& server_endpoint = connection.server_endpoint();

    SECTION("Basic") {
        // Send an upstream message.
        bool sent = client_endpoint.send_message(make_enter_message(14));
        CHECK(sent);

        // Receive it.
        std::span<const Message> messages = server_endpoint.receive_messages(true);
        REQUIRE(messages.size() == 1);
        REQUIRE(messages[0].type == MessageType::ENTER);
        CHECK(messages[0].enter.cycle == 14);

        // Send a downstream message.
        sent = server_endpoint.send_message(make_leave_message(true));
        CHECK(sent);

        // Receive it.
        messages = client_endpoint.receive_messages(true);
        REQUIRE(messages.size() == 1);
        REQUIRE(messages[0].type == MessageType::LEAVE);
        CHECK(messages[0].leave.stop);
    }

    SECTION("Queuing") {
        client_endpoint.send_message(make_enter_message(100));
        client_endpoint.send_message(make_enter_message(200));

        std::span<const Message> messages = server_endpoint.receive_messages(true);
        REQUIRE(messages.size() == 2);
        REQUIRE(messages[0].type == MessageType::ENTER);
        REQUIRE(messages[1].type == MessageType::ENTER);
        CHECK(messages[0].enter.cycle == 100);
        CHECK(messages[1].enter.cycle == 200);
    }

    SECTION("Full") {
        auto message = make_enter_message(0);

        // Fill the server_endpoint's RX stream.
        for (size_t i = 0; i < STREAM_CAPACITY; ++i) {
            CHECK(client_endpoint.send_message(message));
        }
        CHECK(!client_endpoint.send_message(message));

        // Read everything and ensure the stream was exhausted.
        auto messages = server_endpoint.receive_messages(true);
        CHECK(messages.size() == STREAM_CAPACITY);
        CHECK(server_endpoint.receive_messages(true).empty());
    }

    SECTION("Underflow") {
        CHECK(server_endpoint.receive_messages(true).size() == 0);
    }

}
