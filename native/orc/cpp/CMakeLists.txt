cmake_minimum_required(VERSION 3.25)
project(StreamFusionOrc LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
set(BUILD_SHARED_LIBS OFF)
set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER)
include(FetchContent)

# ORC's release defaults to a Git revision of zlib. Use a published archive instead.
FetchContent_Declare(ZLIB
  URL
    https://github.com/madler/zlib/releases/download/v1.3.2/zlib-1.3.2.tar.gz
    https://zlib.net/fossils/zlib-1.3.2.tar.gz
  URL_HASH SHA256=bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16)
set(BUILD_JAVA OFF CACHE BOOL "" FORCE)
set(BUILD_CPP_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(STOP_BUILD_ON_WARNING OFF CACHE BOOL "" FORCE)
# ORC 2.3.1's per-target PIC branch has malformed set_target_properties calls.
# The global CMake setting above already enables PIC for every static dependency.
set(BUILD_POSITION_INDEPENDENT_LIB OFF CACHE BOOL "" FORCE)
set(INSTALL_VENDORED_LIBS ON CACHE BOOL "" FORCE)
set(protobuf_DEBUG_POSTFIX "" CACHE STRING "" FORCE)
set(ENV{ORC_FORMAT_URL} "https://archive.apache.org/dist/orc/orc-format-1.1.1/orc-format-1.1.1.tar.gz")
# Keep the module self-contained even when the developer has other codec installations.
foreach(dependency PROTOBUF SNAPPY ZLIB ZSTD LZ4)
  unset(ENV{${dependency}_HOME})
endforeach()
FetchContent_Declare(orc
  URL https://archive.apache.org/dist/orc/orc-2.3.1/orc-2.3.1.tar.gz
  URL_HASH SHA256=6ef34f92cd81afbd3ed95ff9e5f31fc573f0e8eb647c2c76b9d0b8f443533128)
# The explicit target path also works when CMake considers a Darwin architecture build cross.
set(PROTOBUF_EXECUTABLE "$<TARGET_FILE:protobuf::protoc>")
if(APPLE AND NOT "${SF_HOST_ARCH}" STREQUAL "${CMAKE_OSX_ARCHITECTURES}")
  # Both supported Darwin ABIs have 64-bit time_t; CMake cannot run the target probes.
  set(HAS_PRE_1970_EXITCODE 0 CACHE STRING "" FORCE)
  set(HAS_POST_2038_EXITCODE 0 CACHE STRING "" FORCE)
endif()
FetchContent_MakeAvailable(orc)
if(APPLE AND NOT "${SF_HOST_ARCH}" STREQUAL "${CMAKE_OSX_ARCHITECTURES}")
  # protoc must execute on the build host. A universal host tool avoids requiring Rosetta while
  # its runtime library also supplies the requested Rust target architecture.
  foreach(protobuf_target protoc libprotoc libprotobuf js_embed)
    set_property(TARGET ${protobuf_target} PROPERTY OSX_ARCHITECTURES "${SF_HOST_ARCH};${CMAKE_OSX_ARCHITECTURES}")
  endforeach()
  # Protobuf's generator invokes js_embed by target name. Tell CMake that this universal
  # executable runs directly on the host even though the library build is cross-compiling.
  set_property(TARGET js_embed PROPERTY CROSSCOMPILING_EMULATOR /usr/bin/env)
endif()

set(NANOARROW_INSTALL_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_Declare(nanoarrow
  URL https://archive.apache.org/dist/arrow/apache-arrow-nanoarrow-0.8.0/apache-arrow-nanoarrow-0.8.0.tar.gz
  URL_HASH SHA512=98f9f4c8dada0175e39e02d2baa01d0f63ad94636925cd289cbffa423de26bf0ede437aaa1ec10ff91e7d375e72cfddd950d040602520ab7891ab4c6337ce4f7)
FetchContent_MakeAvailable(nanoarrow)

add_library(streamfusion_orc_cpp STATIC bridge.cc)
target_link_libraries(streamfusion_orc_cpp PRIVATE orc nanoarrow_static)
install(TARGETS streamfusion_orc_cpp ARCHIVE DESTINATION lib)

option(SF_ORC_READER_COMPARISON "Build opt-in reader comparison adapters" OFF)
if(SF_ORC_READER_COMPARISON)
  include(reader_comparison.cmake)
endif()
