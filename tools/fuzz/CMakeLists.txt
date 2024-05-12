add_executable(
    fuzz

    fuzz.cpp
)

target_link_libraries(fuzz PUBLIC mantle)
target_compile_features(fuzz PRIVATE cxx_std_20)

