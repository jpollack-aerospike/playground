cmake_minimum_required(VERSION 3.18)

project(osm_demo LANGUAGES C CXX)

set(OPENSSL_USE_STATIC_LIBS TRUE)
set(CMAKE_CXX_STANDARD 11)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ReadOSM REQUIRED IMPORTED_TARGET readosm)

add_executable(test_osm1 test_osm1.c)
target_link_libraries(test_osm1 PkgConfig::ReadOSM)
 
add_executable(test_osm2 test_osm2.c)
target_link_libraries(test_osm2 PkgConfig::ReadOSM)
 
add_executable(test_osm3 test_osm3.c)
target_link_libraries(test_osm3 PkgConfig::ReadOSM)
