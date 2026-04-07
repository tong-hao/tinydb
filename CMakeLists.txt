cmake_minimum_required(VERSION 3.14)
project(TinyDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# compile options
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
elseif(MSVC)
    add_compile_options(/W4)
endif()

find_package(Threads REQUIRED)

# head file
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/client/include)

# sources
add_subdirectory(src)
add_subdirectory(client)

# google test
find_package(GTest QUIET)
if(GTest_FOUND)
    enable_testing()
    add_subdirectory(tests)
else()
    message(WARNING "GTest not found, tests will not be built")
endif()

# install
install(TARGETS tinydb-server tinydb-cli
    RUNTIME DESTINATION bin
)
