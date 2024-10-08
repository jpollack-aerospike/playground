cmake_minimum_required(VERSION 3.16)
project(toolset LANGUAGES CXX)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_CXX_STANDARD 17)	
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF)
# set(JSONCONS_BUILD_TESTS OFF)
# set(jsoncons_BUILD_TESTS OFF)
# set(BUILD_TESTING OFF)

fetchcontent_declare(
  docopt
  GIT_REPOSITORY	"https://github.com/docopt/docopt.cpp"
  GIT_TAG		"400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")

fetchcontent_declare(
  hdrhistogram
  GIT_REPOSITORY	"https://github.com/HdrHistogram/HdrHistogram_c"
  GIT_TAG			"0.11.8")

fetchcontent_declare(
  nlohmann_json
  GIT_REPOSITORY        "https://github.com/nlohmann/json.git"
  GIT_TAG               "v3.11.2")

fetchcontent_declare(
  concurrentqueue
  GIT_REPOSITORY	"https://github.com/cameron314/concurrentqueue.git"
  GIT_TAG		"master")

# fetchcontent_declare(
#   jsoncons
#   GIT_REPOSITORY	"https://github.com/danielaparker/jsoncons.git"
#   GIT_TAG		"v0.170.1")

set(AEROSPIKE_DIR "${CMAKE_CURRENT_BINARY_DIR}/aerospike-client-c-prefix")

externalproject_add(
  aerospike-client-c
  GIT_REPOSITORY       "https://github.com/aerospike/aerospike-client-c.git"
  GIT_TAG              "6.5.0"
  BUILD_IN_SOURCE	TRUE
  CONFIGURE_COMMAND	""
  INSTALL_COMMAND	make install INSTALL_PREFIX=${AEROSPIKE_DIR}
)

add_library(aerospike INTERFACE)
target_include_directories(aerospike INTERFACE "${AEROSPIKE_DIR}/include")
target_link_libraries(aerospike INTERFACE "${AEROSPIKE_DIR}/lib/libaerospike.a" dl OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)
add_dependencies(aerospike aerospike-client-c)

fetchcontent_makeavailable(nlohmann_json)
# fetchcontent_makeavailable(jsoncons)
fetchcontent_makeavailable(docopt)
fetchcontent_makeavailable(hdrhistogram)
fetchcontent_makeavailable(concurrentqueue)

add_executable(load load.cpp)
target_link_libraries(load docopt aerospike concurrentqueue nlohmann_json::nlohmann_json Threads::Threads)

add_executable(dump dump.cpp)
target_link_libraries(dump docopt aerospike concurrentqueue nlohmann_json::nlohmann_json Threads::Threads)

add_executable(set set.cpp)
target_link_libraries(set docopt aerospike concurrentqueue nlohmann_json::nlohmann_json Threads::Threads)

add_executable(insert insert.cpp)
target_link_libraries(insert docopt aerospike concurrentqueue nlohmann_json::nlohmann_json Threads::Threads)

add_executable(sindex sindex.cpp)
target_link_libraries(sindex aerospike Threads::Threads)

add_executable(delay_put delay_put.cpp)
target_link_libraries(delay_put aerospike Threads::Threads)

add_executable(mmap_block_device mmap_block_device.cpp)
target_link_libraries(mmap_block_device docopt Threads::Threads)

# add_executable(simple_set simple_set.cpp)
# target_link_libraries(simple_set aerospike Threads::Threads)
