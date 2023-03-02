cmake_minimum_required(VERSION 3.18)

project(osmium_apps LANGUAGES CXX)

include(FetchContent)

fetchcontent_declare(
  docopt
  GIT_REPOSITORY	"https://github.com/docopt/docopt.cpp"
  GIT_TAG		"400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(OPENSSL_USE_STATIC_LIBS TRUE)
set(CMAKE_CXX_STANDARD 11)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(EXPAT REQUIRED)
find_package(BZip2 REQUIRED)
find_package(Osmium REQUIRED COMPONENTS io)

include_directories(SYSTEM ${OSMIUM_INCLUDE_DIRS})

fetchcontent_makeavailable(docopt)

add_executable(osmium_load load.cpp)
target_link_libraries(osmium_load docopt Threads::Threads OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB BZip2::BZip2 EXPAT::EXPAT)
