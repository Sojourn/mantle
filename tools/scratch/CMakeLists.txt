add_executable(
    scratch 

    scratch.cpp
)

target_link_libraries(scratch PUBLIC mantle)
target_compile_features(scratch PRIVATE cxx_std_20)

