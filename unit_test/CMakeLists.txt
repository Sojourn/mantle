add_executable(unit_test
        catch.hpp
        unit_test.cpp
        ut_scratch.cpp
        ut_handle.cpp
        ut_object_grouper.cpp
        ut_connection.cpp
        ut_operation.cpp
        ut_operation_ledger.cpp
        ut_sequence_range_history.cpp
        ut_operation_grouper.cpp
        ut_operation_shuffler.cpp
        ut_region_controller.cpp
        )

target_link_libraries(unit_test PUBLIC mantle)
target_compile_features(unit_test PRIVATE cxx_std_20)
