cmake_minimum_required(VERSION 3.16)
project(RobotTrading CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -----------------------------------------------------------------------
# Build instructions
#
# Windows (MSVC + vcpkg):
#   cmake -B build -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake
#   cmake --build build --config Release
#
# Linux (GCC/Clang):
#   cmake -B build -DCMAKE_BUILD_TYPE=Release
#   cmake --build build
#
# Linux real-time prerequisites (one-time):
#   Add to /etc/security/limits.conf:
#       <user>  -  rtprio   99
#       <user>  -  memlock  unlimited
#   Then log out and back in, or run: sudo setcap cap_sys_nice+ep ./robottrading_engine
# -----------------------------------------------------------------------

# -----------------------------------------------------------------------
# IBKR C++ API — place TWS API source files in IBApi/
# Download from: https://interactivebrokers.github.io/#
# Extract the cppclient/client folder contents into IBApi/
# -----------------------------------------------------------------------
set(IBAPI_DIR "${CMAKE_SOURCE_DIR}/IBApi")

file(GLOB IBAPI_SOURCES "${IBAPI_DIR}/*.cpp")
file(GLOB IBAPI_PROTO_SOURCES "${IBAPI_DIR}/protobuf/*.cc")

if(NOT IBAPI_SOURCES)
    message(FATAL_ERROR
        "No IBKR API source files found in ${IBAPI_DIR}.\n"
        "Download TWS API from https://interactivebrokers.github.io/# "
        "and copy the cppclient/client/ files into IBApi/")
endif()

# -----------------------------------------------------------------------
# Protobuf — required by new IBKR API (v9.79+)
# Install via: C:\vcpkg\vcpkg.exe install protobuf:x64-windows
# Then configure with: cmake -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake
# -----------------------------------------------------------------------
find_package(protobuf CONFIG REQUIRED)

# -----------------------------------------------------------------------
# Compiler flags (shared)
# -----------------------------------------------------------------------
if(MSVC)
    add_compile_options(/O2 /Oi /GL /arch:AVX2 /W3
        /wd4267   # size_t → int (protobuf generated code)
        /wd4244   # double → float (protobuf)
        /wd4996   # deprecated functions (sscanf etc)
    )
    add_link_options(/LTCG)
else()
    add_compile_options(-O3 -march=native -funroll-loops -ffast-math -Wall)
endif()

# -----------------------------------------------------------------------
# Sanitizer builds (GCC/Clang only) — for test/dev builds, never production.
# TSan catches the data races the lock-free hot path must avoid:
#   cmake -B build-tsan -DENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug
# -----------------------------------------------------------------------
option(ENABLE_ASAN "Build with AddressSanitizer" OFF)
option(ENABLE_TSAN "Build with ThreadSanitizer"  OFF)
if(NOT MSVC)
    if(ENABLE_ASAN)
        add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=address)
    elseif(ENABLE_TSAN)
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=thread)
    endif()
endif()

# -----------------------------------------------------------------------
# TARGET 1: robottrading_engine
#   Pure trading engine — no UI, no console commands.
#   Exposes ControlServer on localhost:7799.
# -----------------------------------------------------------------------
set(ENGINE_SOURCES
    engine/ThetaDataClient.cpp
    engine/TradingEngine.cpp
    engine/ControlServer.cpp
    engine/bid64_impl.cpp
)

add_executable(robottrading_engine
    main.cpp
    ${ENGINE_SOURCES}
    ${IBAPI_SOURCES}
    ${IBAPI_PROTO_SOURCES}
)

target_include_directories(robottrading_engine PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/engine
    ${IBAPI_DIR}
    ${IBAPI_DIR}/protobuf
)

target_link_libraries(robottrading_engine
    protobuf::libprotobuf
)

if(WIN32)
    target_link_libraries(robottrading_engine ws2_32)
else()
    # pthread: required for pthread_setschedparam, pthread_setaffinity_np
    target_link_libraries(robottrading_engine pthread)
    # _GNU_SOURCE: enables SO_BUSY_POLL and other GNU socket extensions
    target_compile_definitions(robottrading_engine PRIVATE _GNU_SOURCE)
endif()

# -----------------------------------------------------------------------
# TARGET 2: robottrading_ui
#   Web UI process — HTTP server on :8080, connects to engine on :7799.
#   Does NOT need IBApi headers.
# -----------------------------------------------------------------------
set(WEBUI_SOURCES
    webui/HttpServer.cpp
    webui/EngineProxy.cpp
)

add_executable(robottrading_ui
    webui_main.cpp
    ${WEBUI_SOURCES}
)

target_include_directories(robottrading_ui PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/webui
)

if(WIN32)
    target_link_libraries(robottrading_ui ws2_32)
else()
    target_link_libraries(robottrading_ui pthread)
    target_compile_definitions(robottrading_ui PRIVATE _GNU_SOURCE)
endif()

# -----------------------------------------------------------------------
# TARGET 3: robottrading_tests
#   Unit/integration tests for the ThetaData client (mock WS server on
#   localhost). Needs neither IBApi nor protobuf.
#   Run: build\Release\robottrading_tests.exe   (or: ctest --test-dir build)
# -----------------------------------------------------------------------
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_executable(robottrading_tests
        tests/test_thetadata.cpp
        engine/ThetaDataClient.cpp
    )
    target_include_directories(robottrading_tests PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/engine
    )
    if(WIN32)
        target_link_libraries(robottrading_tests ws2_32)
    else()
        target_link_libraries(robottrading_tests pthread)
        target_compile_definitions(robottrading_tests PRIVATE _GNU_SOURCE)
    endif()
    add_test(NAME thetadata COMMAND robottrading_tests)
endif()

# -----------------------------------------------------------------------
# Build type default
# -----------------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "IBKR API dir: ${IBAPI_DIR}")
message(STATUS "Targets: robottrading_engine  robottrading_ui  robottrading_tests")
