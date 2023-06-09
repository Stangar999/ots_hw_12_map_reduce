add_library(version_l version.h
  version.cpp)

target_include_directories(version_l PRIVATE "${CMAKE_BINARY_DIR}")
