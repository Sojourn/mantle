#include "catch.hpp"
#include "mantle/object_grouper.h"

using namespace mantle;

TEST_CASE("ObjectGrouper") {
    ObjectGrouper grouper;

    // Create a few objects belonging to different groups.
    Object object0(0);
    Object object1(1);
    Object object2(1);
    Object object3(3);

    if (ENABLE_OBJECT_GROUPING) {
        for (size_t i = 0; i < 3; ++i) {
            grouper.write(object3);
            grouper.write(object2);
            grouper.write(object0);
            grouper.write(object1);

            ObjectGroups groups = grouper.flush();
            CHECK(groups.object_count == 4);
            CHECK(groups.group_min == 0);
            CHECK(groups.group_max == 3);

            REQUIRE(groups.group_member_count(0) == 1);
            CHECK(groups.group_members(0)[0] == &object0);

            REQUIRE(groups.group_member_count(1) == 2);
            CHECK(groups.group_members(1)[0] == &object1);
            CHECK(groups.group_members(1)[1] == &object2);

            REQUIRE(groups.group_member_count(2) == 0);
            CHECK(groups.group_members(2).empty());

            REQUIRE(groups.group_member_count(3) == 1);
            CHECK(groups.group_members(3)[0] == &object3);
        }
    }
}
