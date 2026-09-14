# Build the released Arrow adapter against the same ORC library as our adapter. This
# selects source files from the unmodified release; no Arrow or ORC patches are applied.
set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "" FORCE)
foreach(option BUILD_TESTS BUILD_BENCHMARKS BUILD_EXAMPLES COMPUTE CSV DATASET FILESYSTEM
    FLIGHT GANDIVA IPC JSON ORC PARQUET WITH_BROTLI WITH_BZ2 WITH_LZ4 WITH_RE2
    WITH_SNAPPY WITH_UTF8PROC WITH_ZLIB WITH_ZSTD JEMALLOC MIMALLOC)
  set(ARROW_${option} OFF CACHE BOOL "" FORCE)
endforeach()
FetchContent_Declare(arrow_cpp
  URL https://archive.apache.org/dist/arrow/arrow-25.0.1/apache-arrow-25.0.1.tar.gz
  URL_HASH SHA512=e75d384b4fdbdee29eb8ad29800c731843e7c43d90a43995dcc77390008723537791e212333178625345c718bdab15e0f3d8c12aa86b336918598c7d3fefc6e5
  SOURCE_SUBDIR cpp)
set(CMAKE_CXX_STANDARD 20)
FetchContent_MakeAvailable(arrow_cpp)
set(CMAKE_CXX_STANDARD 17)
add_library(streamfusion_orc_comparison STATIC
  reader_comparison.cc
  ${arrow_cpp_SOURCE_DIR}/cpp/src/arrow/adapters/orc/adapter.cc
  ${arrow_cpp_SOURCE_DIR}/cpp/src/arrow/adapters/orc/util.cc
  ${arrow_cpp_SOURCE_DIR}/cpp/src/arrow/adapters/orc/options.cc)
set_property(TARGET streamfusion_orc_comparison PROPERTY CXX_STANDARD 20)
target_include_directories(streamfusion_orc_comparison PRIVATE
  ${arrow_cpp_SOURCE_DIR}/cpp/src ${arrow_cpp_BINARY_DIR}/src)
target_link_libraries(streamfusion_orc_comparison PRIVATE arrow_static orc)
install(TARGETS streamfusion_orc_comparison ARCHIVE DESTINATION lib)
