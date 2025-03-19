cmake_minimum_required(VERSION 3.16)
project(dtest LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 17)	
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

# set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF)
# set(JSONCONS_BUILD_TESTS OFF)
# set(jsoncons_BUILD_TESTS OFF)

set(BUILD_TESTING OFF)
set(ROCKSDB_BUILD_TESTS OFF)
set(rocksdb_BUILD_TESTS OFF)

# fetchcontent_declare(
#   docopt
#   GIT_REPOSITORY	"https://github.com/docopt/docopt.cpp"
#   GIT_TAG		"400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")
# 
# fetchcontent_declare(
#   hdrhistogram
#   GIT_REPOSITORY	"https://github.com/HdrHistogram/HdrHistogram_c"
#   GIT_TAG			"0.11.8")
# 
fetchcontent_declare(
  nlohmann_json
  SOURCE_DIR  "/home/joshua/json"
  GIT_TAG	"jpdev")

fetchcontent_declare(
  rocksdb
  GIT_REPOSITORY	"https://github.com/facebook/rocksdb"
  GIT_TAG		"v9.5.2")

fetchcontent_declare(
  cpp-httplib
  GIT_REPOSITORY	"https://github.com/yhirose/cpp-httplib"
  GIT_TAG		"v0.16.3")

# fetchcontent_declare(
#  nlohmann_json
#  GIT_REPOSITORY        "https://github.com/nlohmann/json.git"
#  GIT_TAG               "v3.11.3")

# fetchcontent_declare(
#   concurrentqueue
#   GIT_REPOSITORY	"https://github.com/cameron314/concurrentqueue.git"
#   GIT_TAG		"master")

# fetchcontent_declare(
#   jsoncons
#   GIT_REPOSITORY	"https://github.com/danielaparker/jsoncons.git"
#   GIT_TAG		"v0.170.1")

# set(AEROSPIKE_DIR "${CMAKE_CURRENT_BINARY_DIR}/aerospike-client-c-prefix")
# 
# externalproject_add(
#   aerospike-client-c
#   GIT_REPOSITORY       "https://github.com/aerospike/aerospike-client-c.git"
#   GIT_TAG              "6.5.0"
#   BUILD_IN_SOURCE	TRUE
#   CONFIGURE_COMMAND	""
#   INSTALL_COMMAND	make install INSTALL_PREFIX=${AEROSPIKE_DIR}
# )
# 
# add_library(aerospike INTERFACE)
# target_include_directories(aerospike INTERFACE "${AEROSPIKE_DIR}/include")
# target_link_libraries(aerospike INTERFACE "${AEROSPIKE_DIR}/lib/libaerospike.a" dl OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)
# add_dependencies(aerospike aerospike-client-c)
# 
fetchcontent_makeavailable(nlohmann_json)
fetchcontent_makeavailable(rocksdb)
fetchcontent_makeavailable(cpp-httplib)
# fetchcontent_makeavailable(jsoncons)
# fetchcontent_makeavailable(docopt)
# fetchcontent_makeavailable(hdrhistogram)
# fetchcontent_makeavailable(concurrentqueue)

# add_executable(create_records common.cpp create_records.cpp)
# target_link_libraries(create_records docopt aerospike Threads::Threads)

# add_executable(list_records common.cpp list_records.cpp)
# target_link_libraries(list_records docopt aerospike Threads::Threads)

# add_executable(gharchive_split gharchive_split.cpp)
# target_link_libraries(gharchive_split docopt aerospike nlohmann_json::nlohmann_json Threads::Threads)

# add_executable(query query.cpp)
# target_link_libraries(query docopt aerospike jsoncons Threads::Threads)

# add_executable(getset getset.cpp)
# target_link_libraries(getset docopt aerospike jsoncons concurrentqueue Threads::Threads)

# add_executable(conn cf_ripemd160.c conn.cpp as_proto.cpp)
# target_link_libraries(conn Threads::Threads)
# 
# add_executable(data_init cf_ripemd160.c data_init.cpp as_proto.cpp util.cpp simple_bank.cpp)
# target_link_libraries(data_init Threads::Threads)
# 				  
add_executable(sserver cf_ripemd160.c sserver.cpp as_proto.cpp as_listener.cpp util.cpp)
target_link_libraries(sserver Threads::Threads nlohmann_json::nlohmann_json)

add_executable(info cf_ripemd160.c info.cpp util.cpp as_proto.cpp)
target_link_libraries(info Threads::Threads nlohmann_json::nlohmann_json)

add_executable(mrttest cf_ripemd160.c mrttest.cpp as_proto.cpp util.cpp simple_string.cpp)
target_link_libraries(mrttest Threads::Threads nlohmann_json::nlohmann_json)

add_executable(tstr cf_ripemd160.c tstr.cpp as_proto.cpp util.cpp simple_string.cpp transactional_string.cpp)
target_link_libraries(tstr Threads::Threads nlohmann_json::nlohmann_json)

add_executable(reroster cf_ripemd160.c reroster.cpp as_proto.cpp util.cpp simple_string.cpp)
target_link_libraries(reroster Threads::Threads nlohmann_json::nlohmann_json)

add_executable(string_init cf_ripemd160.c string_init.cpp as_proto.cpp util.cpp simple_string.cpp)
target_link_libraries(string_init Threads::Threads nlohmann_json::nlohmann_json)

add_executable(string_insert cf_ripemd160.c string_insert.cpp as_proto.cpp util.cpp simple_string.cpp)
target_link_libraries(string_insert Threads::Threads nlohmann_json::nlohmann_json)

add_executable(mrt_workload cf_ripemd160.c mrt_workload.cpp as_proto.cpp util.cpp simple_string.cpp transaction.cpp)
target_link_libraries(mrt_workload Threads::Threads nlohmann_json::nlohmann_json)

add_executable(simplest_workload cf_ripemd160.c simplest_workload.cpp as_proto.cpp util.cpp simple_string.cpp transaction.cpp)
target_link_libraries(simplest_workload Threads::Threads nlohmann_json::nlohmann_json)

add_executable(hello_rocks hello_rocks.cpp)
target_link_libraries(hello_rocks rocksdb dl)			   

add_executable(dump dump.cpp cf_ripemd160.c as_proto.cpp util.cpp)
target_link_libraries(dump Threads::Threads nlohmann_json::nlohmann_json)

add_executable(hstest hstest.cpp)
target_link_libraries(hstest httplib::httplib)

add_executable(dore dore.cpp cf_ripemd160.c as_proto.cpp util.cpp)
target_link_libraries(dore Threads::Threads nlohmann_json::nlohmann_json rocksdb dl)
