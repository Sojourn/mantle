add_executable(
    ping_pong

    ping_pong.cpp
)

target_link_libraries(ping_pong PUBLIC mantle)
target_compile_features(ping_pong PRIVATE cxx_std_20)

