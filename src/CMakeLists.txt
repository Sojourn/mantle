set(MANTLE_SOURCE_FILES
    mantle.cpp
    domain.cpp
    region.cpp
    region_controller.cpp
    object.cpp
    ledger.cpp
    operation_grouper.cpp
    doorbell.cpp
    selector.cpp
    page_fault_handler.cpp
)

set(MANTLE_HEADER_FILES
)

add_library(mantle ${MANTLE_SOURCE_FILES} ${MANTLE_HEADER_FILES})
target_link_libraries(mantle fmt)
target_include_directories(mantle
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
        PRIVATE
            $<CMAKE_CURRENT_SOURCE_DIR}
        )

set_target_properties(mantle PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO
        )
