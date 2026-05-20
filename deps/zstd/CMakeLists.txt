project(zstd)

set(SRCS
    "${CMAKE_CURRENT_LIST_DIR}/zstd.c"
    "${CMAKE_CURRENT_LIST_DIR}/zstd.h"
    "${CMAKE_CURRENT_LIST_DIR}/zstd_errors.h")

add_library(zstd STATIC ${SRCS})
set_target_properties(zstd PROPERTIES POSITION_INDEPENDENT_CODE ON)
