 cmake_minimum_required(VERSION 3.18)

project(simple_client LANGUAGES CXX)
include(ExternalProject)

set(EXTERN_DIR ${CMAKE_CURRENT_BINARY_DIR}/extern)

ExternalProject_Add(
  aerospike-client-c
  GIT_REPOSITORY       "https://github.com/aerospike/aerospike-client-c.git"
  GIT_TAG              "6.3.1"
  BUILD_IN_SOURCE      TRUE
  BUILD_COMMAND        make
  INSTALL_COMMAND      make install INSTALL_PREFIX=${EXTERN_DIR}
  CONFIGURE_COMMAND    ""
)

add_library(aerospike STATIC IMPORTED)
set_target_properties(aerospike PROPERTIES IMPORTED_LOCATION "${EXTERN_DIR}/lib/libaerospike.a")
add_dependencies(aerospike aerospike-client-c)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
include_directories(${EXTERN_DIR}/include)

add_executable(simple_client main.cpp)
target_link_libraries(simple_client aerospike Threads::Threads OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB dl)
