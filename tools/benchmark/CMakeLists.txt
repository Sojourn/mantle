add_executable(
    benchmark

    benchmark.cpp
)

target_link_libraries(benchmark PUBLIC mantle)
target_compile_features(benchmark PRIVATE cxx_std_20)

