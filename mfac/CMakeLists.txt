cmake_minimum_required(VERSION 3.18)

project(mfac LANGUAGES CXX)
set(EXTERN_DIR ${CMAKE_CURRENT_BINARY_DIR}/extern)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

fetchcontent_declare(
  docopt
  GIT_REPOSITORY	"https://github.com/docopt/docopt.cpp"
  GIT_TAG		"400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")

externalproject_add(
  aerospike-client-c
  GIT_REPOSITORY       "https://github.com/aerospike/aerospike-client-c.git"
  GIT_TAG              "6.3.1"
  BUILD_IN_SOURCE	TRUE
  CONFIGURE_COMMAND	""
  BUILD_COMMAND		make
  INSTALL_COMMAND	make install INSTALL_PREFIX=${EXTERN_DIR}
)

find_library(AEROSPIKE NAMES aerospike PATHS "${EXTERN_DIR}/lib")

add_library(aerospike INTERFACE)
target_include_directories(aerospike INTERFACE "${EXTERN_DIR}/include")
target_link_libraries(aerospike INTERFACE ${AEROSPIKE} dl OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)
add_dependencies(aerospike aerospike-client-c)

fetchcontent_makeavailable(docopt)

add_executable(create_records common.cpp create_records.cpp)
target_link_libraries(create_records docopt aerospike Threads::Threads)

add_executable(list_records common.cpp list_records.cpp)
target_link_libraries(list_records docopt aerospike Threads::Threads)
