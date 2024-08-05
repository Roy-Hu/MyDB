cmake_minimum_required(VERSION 3.5)
project(MyProject)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O3")

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/Main/Catalog/headers
    ${CMAKE_SOURCE_DIR}/Main/BufferMgr/headers
    ${CMAKE_SOURCE_DIR}/Main/Execution/headers
    ${CMAKE_SOURCE_DIR}/Main/Qunit/headers
    ${CMAKE_SOURCE_DIR}/Main/Record/headers
    ${CMAKE_SOURCE_DIR}/Main/DatabaseTable/headers
    ${CMAKE_SOURCE_DIR}/Main/SQL/headers
    ${CMAKE_BINARY_DIR}/Main/SQL/source  # Include directory for generated files
    ${CMAKE_SOURCE_DIR}/Main/RelOps/headers
)

# Source directories
file(GLOB_RECURSE CATALOG_SRC ${CMAKE_SOURCE_DIR}/Main/Catalog/source/*.cc)
file(GLOB_RECURSE BUFFER_SRC ${CMAKE_SOURCE_DIR}/Main/BufferMgr/source/*.cc)
file(GLOB_RECURSE EXECUTION_SRC ${CMAKE_SOURCE_DIR}/Main/Execution/source/*.cc)
file(GLOB_RECURSE RECORD_SRC ${CMAKE_SOURCE_DIR}/Main/Record/source/*.cc)
file(GLOB_RECURSE TABLE_SRC ${CMAKE_SOURCE_DIR}/Main/DatabaseTable/source/*.cc)
file(GLOB_RECURSE COMMON_SRC ${CMAKE_SOURCE_DIR}/Main/Common/source/*.cc)
file(GLOB_RECURSE RELOP_SRC ${CMAKE_SOURCE_DIR}/Main/RelOps/source/*.cc)

# Output binaries to Build/bin directory
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/Build/bin)

# Generate lexer and parser
find_package(BISON)
find_package(FLEX)

BISON_TARGET(Parser ${CMAKE_SOURCE_DIR}/Main/SQL/source/Parser.y ${CMAKE_BINARY_DIR}/Main/SQL/source/Parser.c COMPILE_FLAGS "-d")
FLEX_TARGET(Lexer ${CMAKE_SOURCE_DIR}/Main/SQL/source/Lexer.l ${CMAKE_BINARY_DIR}/Main/SQL/source/Lexer.c)
ADD_FLEX_BISON_DEPENDENCY(Lexer Parser)

# Source files for SQL parser
set(SQL_SRC
    ${BISON_Parser_OUTPUT_SOURCE}
    ${FLEX_Lexer_OUTPUTS}
    ${CMAKE_SOURCE_DIR}/Main/SQL/source/ParserHelperFunctions.cc
)

# Function to add tests
function(add_unit_test TARGET_NAME SOURCE_FILES)
    add_executable(${TARGET_NAME} ${SOURCE_FILES} ${COMMON_SRC} ${CATALOG_SRC} ${BUFFER_SRC} ${EXECUTION_SRC} ${RECORD_SRC} ${TABLE_SRC} ${SQL_SRC} ${RELOP_SRC})
    target_include_directories(${TARGET_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/Main/Catalog/headers
        ${CMAKE_SOURCE_DIR}/Main/BufferMgr/headers
        ${CMAKE_SOURCE_DIR}/Main/Execution/headers
        ${CMAKE_SOURCE_DIR}/Main/Qunit/headers
        ${CMAKE_SOURCE_DIR}/Main/Record/headers
        ${CMAKE_SOURCE_DIR}/Main/DatabaseTable/headers
        ${CMAKE_SOURCE_DIR}/Main/SQL/headers
        ${CMAKE_BINARY_DIR}/Main/SQL/source  # Include directory for generated files
        ${CMAKE_SOURCE_DIR}/Main/RelOps/headers
    )
endfunction()

# Building all targets
add_unit_test(bufferUnitTest ${CMAKE_SOURCE_DIR}/Main/BufferTest/source/BufferQUnit.cc)
add_unit_test(recordUnitTest ${CMAKE_SOURCE_DIR}/Main/RecordTest/source/RecordQUnit.cc)
add_unit_test(sortUnitTest ${CMAKE_SOURCE_DIR}/Main/SortTest/source/SortQUnit.cc)
add_unit_test(bPlusUnitTest ${CMAKE_SOURCE_DIR}/Main/BPlusTest/source/BPlusQUnit.cc)
add_unit_test(sqlUnitTest ${CMAKE_SOURCE_DIR}/Main/SQLTest/source/main.cc)
add_unit_test(relOpUnitTest ${CMAKE_SOURCE_DIR}/Main/RelOpTest/source/RelOpQUnit.cc)
