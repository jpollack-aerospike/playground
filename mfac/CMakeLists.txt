cmake_minimum_required(VERSION 3.18)
project(mfac LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)	
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

set(JSONCONS_BUILD_TESTS OFF)
set(jsoncons_BUILD_TESTS OFF)
set(BUILD_TESTING OFF)

fetchcontent_declare(
  docopt
  GIT_REPOSITORY	"https://github.com/docopt/docopt.cpp"
  GIT_TAG		"400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")

fetchcontent_declare(
  nlohmann_json
  GIT_REPOSITORY        "https://github.com/nlohmann/json.git"
  GIT_TAG               "v3.11.2")

fetchcontent_declare(
  concurrentqueue
  GIT_REPOSITORY	"https://github.com/cameron314/concurrentqueue.git"
  GIT_TAG		"master")

fetchcontent_declare(
  jsoncons
  GIT_REPOSITORY	"https://github.com/danielaparker/jsoncons.git"
  GIT_TAG		"v0.170.1")

set(AEROSPIKE_DIR "${CMAKE_CURRENT_BINARY_DIR}/aerospike-client-c-prefix")

externalproject_add(
  aerospike-client-c
  GIT_REPOSITORY       "https://github.com/aerospike/aerospike-client-c.git"
  GIT_TAG              "6.3.1"
  BUILD_IN_SOURCE	TRUE
  CONFIGURE_COMMAND	""
  INSTALL_COMMAND	make install INSTALL_PREFIX=${AEROSPIKE_DIR}
)

add_library(aerospike INTERFACE)
target_include_directories(aerospike INTERFACE "${AEROSPIKE_DIR}/include")
target_link_libraries(aerospike INTERFACE "${AEROSPIKE_DIR}/lib/libaerospike.a" dl OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)
add_dependencies(aerospike aerospike-client-c)

fetchcontent_makeavailable(nlohmann_json)
fetchcontent_makeavailable(jsoncons)
fetchcontent_makeavailable(docopt)
fetchcontent_makeavailable(concurrentqueue)

add_executable(create_records common.cpp create_records.cpp)
target_link_libraries(create_records docopt aerospike Threads::Threads)

add_executable(list_records common.cpp list_records.cpp)
target_link_libraries(list_records docopt aerospike Threads::Threads)

add_executable(gharchive_split gharchive_split.cpp)
target_link_libraries(gharchive_split docopt aerospike nlohmann_json::nlohmann_json Threads::Threads)

add_executable(query query.cpp)
target_link_libraries(query docopt aerospike jsoncons Threads::Threads)

add_executable(getset getset.cpp)
target_link_libraries(getset docopt aerospike jsoncons concurrentqueue Threads::Threads)

# add_executable(asquery asquery.cpp)
# target_link_libraries(asquery docopt aerospike jsoncons concurrentqueue Threads::Threads)
