include_directories(include)

include_directories(${PROJECT_SOURCE_DIR}/libgpopt/include/)
include_directories(${PROJECT_SOURCE_DIR}/libnaucrates/include/)

file(GLOB_RECURSE hdrs ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

include(CheckCXXSourceCompiles)
include(CMakePushCheckState)

# While attempting to compile a function with the format attribute, ensure that:
# warnings are treated as errors and
# unrecognized attributes are warned
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_FLAGS -Werror -Wattribute)
check_cxx_source_compiles("int main() {}
    extern int pgac_write(int ignore, const char *fmt,...)
    __attribute__((format(gnu_printf, 2, 3)));" COMPILER_SUPPORTS_GNU_PRINTF)
cmake_pop_check_state()

if(COMPILER_SUPPORTS_GNU_PRINTF)
    set(GPOS_FORMAT_ARCHETYPE gnu_printf)
else()
    set(GPOS_FORMAT_ARCHETYPE printf)
endif()

configure_file(config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/gpos/config.h)

add_library(gpos ${srcs})

target_link_libraries(gpos ${CMAKE_DL_LIBS})
target_include_directories(gpos PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include)

# Tests reside in the 'server' subdirectory.
add_subdirectory(server)
