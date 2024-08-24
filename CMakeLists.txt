cmake_minimum_required(VERSION 3.5)
project(MyProject)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

# Include directories
include_directories(
    Main/Catalog/headers
    Main/BufferMgr/headers
    Main/Execution/headers
    Main/Qunit/headers
    Main/Record/headers
    Main/DatabaseTable/headers
    Main/SQL/headers
    ${CMAKE_BINARY_DIR}/Main/SQL/source  # Include directory for generated files
    Main/RelOps/headers
)

# Source directories
file(GLOB_RECURSE CATALOG_SRC Main/Catalog/source/*.cc)
file(GLOB_RECURSE BUFFER_SRC Main/BufferMgr/source/*.cc)
file(GLOB_RECURSE EXECUTION_SRC Main/Execution/source/*.cc)
file(GLOB_RECURSE RECORD_SRC Main/Record/source/*.cc)
file(GLOB_RECURSE TABLE_SRC Main/DatabaseTable/source/*.cc)
file(GLOB_RECURSE COMMON_SRC Main/Common/source/*.cc)
file(GLOB_RECURSE RELOP_SRC Main/RelOps/source/*.cc)

# Output binaries to Build/bin directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/Build/bin)

# Generate lexer and parser
find_package(BISON)
find_package(FLEX)

BISON_TARGET(Parser Main/SQL/source/Parser.y ${CMAKE_BINARY_DIR}/Main/SQL/source/Parser.c COMPILE_FLAGS "-d")
FLEX_TARGET(Lexer Main/SQL/source/Lexer.l ${CMAKE_BINARY_DIR}/Main/SQL/source/Lexer.c)
ADD_FLEX_BISON_DEPENDENCY(Lexer Parser)

# Source files for SQL parser
set(SQL_SRC
    ${BISON_Parser_OUTPUT_SOURCE}
    ${FLEX_Lexer_OUTPUTS}
    Main/SQL/source/ParserHelperFunctions.cc
)

# Main program
add_executable(sqlProgram Main/SQLTest/source/main.cc ${COMMON_SRC} ${CATALOG_SRC} ${BUFFER_SRC} ${EXECUTION_SRC} ${RECORD_SRC} ${TABLE_SRC} ${SQL_SRC} ${RELOP_SRC})
target_include_directories(sqlProgram PRIVATE
    Main/Catalog/headers
    Main/BufferMgr/headers
    Main/Execution/headers
    Main/Qunit/headers
    Main/Record/headers
    Main/DatabaseTable/headers
    Main/SQL/headers
    ${CMAKE_BINARY_DIR}/Main/SQL/source  # Include directory for generated files
    Main/RelOps/headers
)

# Function to add unit tests
function(add_unit_test TARGET_NAME SOURCE_FILES)
    add_executable(${TARGET_NAME} ${SOURCE_FILES} ${COMMON_SRC} ${CATALOG_SRC} ${BUFFER_SRC} ${EXECUTION_SRC} ${RECORD_SRC} ${TABLE_SRC} ${SQL_SRC} ${RELOP_SRC})
    target_include_directories(${TARGET_NAME} PRIVATE
        Main/Catalog/headers
        Main/BufferMgr/headers
        Main/Execution/headers
        Main/Qunit/headers
        Main/Record/headers
        Main/DatabaseTable/headers
        Main/SQL/headers
        ${CMAKE_BINARY_DIR}/Main/SQL/source  # Include directory for generated files
        Main/RelOps/headers
    )
    add_test(NAME ${TARGET_NAME} 
             COMMAND ${CMAKE_COMMAND} -E env 
                     CTEST_OUTPUT_ON_FAILURE=1 
                     ${CMAKE_CURRENT_BINARY_DIR}/Build/bin/${TARGET_NAME})
    set_tests_properties(${TARGET_NAME} PROPERTIES 
                         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/Build)
endfunction()

# Option to build unit tests
option(BUILD_UNIT_TESTS "Build the unit tests" OFF)

if(BUILD_UNIT_TESTS)
    enable_testing()
    
    # Add unit tests
    add_unit_test(bufferUnitTest Main/BufferTest/source/BufferQUnit.cc)
    add_unit_test(recordUnitTest Main/RecordTest/source/RecordQUnit.cc)
    add_unit_test(sortUnitTest Main/SortTest/source/SortQUnit.cc)
    add_unit_test(bPlusUnitTest Main/BPlusTest/source/BPlusQUnit.cc)
    add_unit_test(relOpUnitTest Main/RelOpTest/source/RelOpQUnit.cc)

    # Custom target for running all tests
    add_custom_target(test_all
        COMMAND ${CMAKE_CTEST_COMMAND} -V
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/Build
        DEPENDS bufferUnitTest recordUnitTest sortUnitTest bPlusUnitTest relOpUnitTest
    )
endif()
