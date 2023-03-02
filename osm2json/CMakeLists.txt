cmake_minimum_required(VERSION 3.18)

project(osm2json LANGUAGES C CXX)

set(OPENSSL_USE_STATIC_LIBS TRUE)
set(CMAKE_CXX_STANDARD 11)

include(FetchContent)
fetchcontent_declare(
  docopt
  GIT_REPOSITORY        "https://github.com/docopt/docopt.cpp"
  GIT_TAG               "400e6dd8e59196c914dcc2c56caf7dae7efa5eb3")

fetchcontent_declare(
  nlohmann_json
  GIT_REPOSITORY        "https://github.com/nlohmann/json.git"
  GIT_TAG               "v3.11.2")

fetchcontent_makeavailable(docopt)
fetchcontent_makeavailable(nlohmann_json)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ReadOSM REQUIRED IMPORTED_TARGET readosm)

add_executable(osm2json main.cpp)
target_compile_definitions(osm2json PRIVATE JSON_NOEXCEPTION)
target_link_libraries(osm2json PkgConfig::ReadOSM docopt nlohmann_json::nlohmann_json)
