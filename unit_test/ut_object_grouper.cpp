#include "catch.hpp"
#include "mantle/object_grouper.h"

using namespace mantle;

TEST_CASE("ObjectGrouper") {
    ObjectGrouper grouper;

    if (MANTLE_ENABLE_OBJECT_GROUPING) {
        SECTION("Repetition") {
            Object object0(0);
            Object object1(1);
            Object object2(1);
            Object object3(3);

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

        SECTION("Multiple groups") {
            Object objects[] = {
                Object{1},
                Object{2},
                Object{2},
                Object{3},
                Object{3},
                Object{3},
                Object{4},
                Object{4},
                Object{4},
                Object{4},
            };

            for (Object& object : objects) {
                grouper.write(object);
            }

            ObjectGroups groups = grouper.flush();
            CHECK(groups.object_count == 10);
            CHECK(groups.group_min == 1);
            CHECK(groups.group_max == 4);

            CHECK(groups.group_member_count(0) == 0);
            CHECK(groups.group_member_count(1) == 1);
            CHECK(groups.group_member_count(2) == 2);
            CHECK(groups.group_member_count(3) == 3);

            // TODO: Treat groups as sets and check set membership.
            //       Currently it relies on implementation details (group ordering).
            if (1) {
                CHECK(groups.group_members(1)[0] == &objects[0]);
                CHECK(groups.group_members(2)[1] == &objects[1]);
                CHECK(groups.group_members(2)[0] == &objects[2]);
                CHECK(groups.group_members(3)[2] == &objects[3]);
                CHECK(groups.group_members(3)[1] == &objects[4]);
                CHECK(groups.group_members(3)[0] == &objects[5]);
                CHECK(groups.group_members(4)[3] == &objects[6]);
                CHECK(groups.group_members(4)[2] == &objects[7]);
                CHECK(groups.group_members(4)[1] == &objects[8]);
                CHECK(groups.group_members(4)[0] == &objects[9]);
            }
        }
    }
}
