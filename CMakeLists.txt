cmake_minimum_required(VERSION 3.14)
project(TinyDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 编译选项
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
elseif(MSVC)
    add_compile_options(/W4)
endif()

# 查找依赖包
find_package(Threads REQUIRED)

# 公共头文件目录
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/client/include)

# 源文件目录
add_subdirectory(src)
add_subdirectory(client)

# 启用测试（可选）
find_package(GTest QUIET)
if(GTest_FOUND)
    enable_testing()
    add_subdirectory(tests)
else()
    message(WARNING "GTest not found, tests will not be built")
endif()

# 安装目标
install(TARGETS tinydb-server tinydb-cli
    RUNTIME DESTINATION bin
)
