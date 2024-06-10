#
#  Copyright 2008 Search Solution Corporation
#  Copyright 2016 CUBRID Corporation
# 
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
# 
#       http://www.apache.org/licenses/LICENSE-2.0
# 
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# 
#

#set(BROKER_DEFS _BSD_SOURCE _XOPEN_SOURCE ASYNC_MODE CAS_BROKER)
set(BROKER_DEFS _XOPEN_SOURCE ASYNC_MODE CAS_BROKER)

set(CUB_BROKER_SOURCES
  ${BROKER_DIR}/broker.c
  ${BROKER_DIR}/broker_shm.c
  ${BROKER_DIR}/broker_error.c
  ${BROKER_DIR}/broker_util.c
  ${BROKER_DIR}/broker_process_size.c
  ${BROKER_DIR}/broker_max_heap.c
  ${BROKER_DIR}/broker_access_list.c
  ${BROKER_DIR}/broker_filename.c
  )

if(UNIX)
  list(APPEND CUB_BROKER_SOURCES ${BROKER_DIR}/broker_send_fd.c)
  list(APPEND CUB_BROKER_SOURCES ${BASE_DIR}/porting.c)
  list(APPEND CUB_BROKER_SOURCES ${BASE_DIR}/environment_variable.c)
  SET_SOURCE_FILES_PROPERTIES(
    ${CUB_BROKER_SOURCES}
    PROPERTIES LANGUAGE CXX
  )
endif(UNIX)
# cub_broker use WinMain() on Windows platform
#add_executable(cub_broker WIN32 ${CUB_BROKER_SOURCES})
add_executable(api_broker ${CUB_BROKER_SOURCES})
#set_target_properties(api_broker PROPERTIES PUBLIC_HEADER "${COMPAT_DIR}/dbtran_def.h;${BROKER_DIR}/cas_error.h")
set_target_properties(api_broker PROPERTIES PUBLIC_HEADER "${BROKER_DIR}/cas_error.h")
target_compile_definitions(api_broker PRIVATE ${BROKER_DEFS} FOR_API_BROKER)
target_include_directories(api_broker PRIVATE ${BASE_DIR})
if(UNIX)
  target_link_libraries(api_broker LINK_PRIVATE m)
endif(UNIX)

set(BROKER_MONITOR_SOURCES
  ${BROKER_DIR}/broker_monitor.c
  ${BROKER_DIR}/broker_config.c
  ${BROKER_DIR}/broker_shm.c
  ${BROKER_DIR}/broker_error.c
  ${BROKER_DIR}/broker_util.c
  ${BROKER_DIR}/broker_process_size.c
  ${BROKER_DIR}/broker_max_heap.c
  ${BROKER_DIR}/broker_process_info.c
  ${BROKER_DIR}/broker_filename.c
  ${BROKER_DIR}/cas_util.c
  ${BASE_DIR}/util_func.c
  ${BASE_DIR}/ini_parser.c
  ${BASE_DIR}/chartype.c
  ${BASE_DIR}/porting.c
  ${BASE_DIR}/environment_variable.c
  )
SET_SOURCE_FILES_PROPERTIES(
  ${BROKER_MONITOR_SOURCES}
  PROPERTIES LANGUAGE CXX
  )

add_executable(broker_monitor ${BROKER_MONITOR_SOURCES})
target_compile_definitions(broker_monitor PRIVATE ${BROKER_DEFS} FOR_API_BROKER FOR_API_CAS)
target_include_directories(broker_monitor PRIVATE ${BASE_DIR})
target_link_libraries(broker_monitor LINK_PRIVATE -ldl)

set(CUBRID_BROKER_SOURCES
  ${BASE_DIR}/porting.c
  ${BROKER_DIR}/broker_config.c
  ${BROKER_DIR}/broker_admin.c
  ${BROKER_DIR}/broker_shm.c
  ${BROKER_DIR}/broker_error.c
  ${BROKER_DIR}/broker_util.c
  ${BROKER_DIR}/broker_admin_pub.c
  ${BROKER_DIR}/broker_process_size.c
  ${BROKER_DIR}/broker_max_heap.c
  ${BROKER_DIR}/broker_filename.c
  ${BROKER_DIR}/broker_acl.c
  ${BASE_DIR}/util_func.c
  ${BASE_DIR}/ini_parser.c
  ${BASE_DIR}/chartype.c
  ${BASE_DIR}/porting.c
  ${BASE_DIR}/environment_variable.c
  )
SET_SOURCE_FILES_PROPERTIES(
  ${CUBRID_BROKER_SOURCES}
  PROPERTIES LANGUAGE CXX
  )
add_executable(brokerutil ${CUBRID_BROKER_SOURCES})
target_compile_definitions(brokerutil PRIVATE ${BROKER_DEFS} FOR_API_BROKER FOR_API_CAS)
target_include_directories(brokerutil PRIVATE ${BASE_DIR})
if(UNIX)
  target_link_libraries(brokerutil LINK_PRIVATE m)
endif(UNIX)

set(CUB_CAS_SOURCES
  ${BROKER_DIR}/cas.c
  ${BROKER_DIR}/cas_network.c
  ${BROKER_DIR}/cas_net_buf.c
  ${BROKER_DIR}/cas_function.c
  ${BROKER_DIR}/cas_execute.c
  ${BROKER_DIR}/cas_handle.c
  ${BROKER_DIR}/broker_util.c
  ${BROKER_DIR}/cas_str_like.c
  ${BROKER_DIR}/cas_schema_info.c
  ${BROKER_DIR}/cas_log.c
  ${BROKER_DIR}/broker_shm.c
  ${BROKER_DIR}/cas_sql_log2.c
  ${BROKER_DIR}/broker_filename.c
  ${BROKER_DIR}/cas_error.c
  ${BROKER_DIR}/cas_error_log.c
  ${BROKER_DIR}/broker_acl.c
  ${BROKER_DIR}/cas_meta.c
  ${BROKER_DIR}/cas_util.c
  ${BROKER_DIR}/cas_dbms_util.c
  )

if(UNIX)
  list(APPEND CUB_CAS_SOURCES ${BROKER_DIR}/broker_error.c)
  list(APPEND CUB_CAS_SOURCES ${BROKER_DIR}/broker_process_size.c)
  list(APPEND CUB_CAS_SOURCES ${BROKER_DIR}/broker_recv_fd.c)
  list(APPEND CUB_CAS_SOURCES ${S62_DIR}/s62ext.c)
  list(APPEND CUB_CAS_SOURCES ${BASE_DIR}/environment_variable.c)
  list(APPEND CUB_CAS_SOURCES ${BASE_DIR}/porting.c)
  SET_SOURCE_FILES_PROPERTIES(
    ${CUB_CAS_SOURCES}
    PROPERTIES LANGUAGE CXX
  )
endif(UNIX)

add_executable(api_cas WIN32 ${CUB_CAS_SOURCES})
target_compile_definitions(api_cas PRIVATE ${BROKER_DEFS} FOR_API_BROKER FOR_API_CAS CAS_ERROR_LOG)
target_include_directories(api_cas PRIVATE ${EP_INCLUDES})
target_include_directories(api_cas PRIVATE ${BASE_DIR})
target_include_directories(api_cas PRIVATE ${S62_DIR})

find_library (
  S62LIB
  NAMES libtbgpp-api-server-shared.so
  HINTS /turbograph-v3/turbograph-v3/build/tbgpp-api-server
  REQUIRED)

target_link_libraries(api_cas ${S62LIB})

#set(BROKERADMIN_SOURCES
#  ${BROKER_DIR}/broker_admin_so.c
#  ${BROKER_DIR}/broker_admin_pub.c
#  ${BROKER_DIR}/broker_shm.c
#  ${BROKER_DIR}/broker_error.c
#  ${BROKER_DIR}/broker_util.c
#  ${BROKER_DIR}/broker_config.c
#  ${BROKER_DIR}/broker_max_heap.c
#  ${BROKER_DIR}/broker_process_size.c
#  ${BROKER_DIR}/broker_process_info.c
#  ${BROKER_DIR}/broker_filename.c
#  ${BROKER_DIR}/broker_acl.c
#  )
#SET_SOURCE_FILES_PROPERTIES(
#  ${BROKERADMIN_SOURCES}
#  PROPERTIES LANGUAGE CXX
#  )
#add_library(brokeradmin SHARED ${BROKERADMIN_SOURCES})
#set_target_properties(brokeradmin PROPERTIES SOVERSION "${CUBRID_MAJOR_VERSION}.${CUBRID_MINOR_VERSION}")
#target_compile_definitions(brokeradmin PRIVATE ${BROKER_DEFS} _UC_ADMIN_SO_ FOR_API_BROKER FOR_API_CAS)
#target_include_directories(brokeradmin PRIVATE ${BASE_DIR})

install(TARGETS
#  tbgpp-api-server
  api_broker
  broker_monitor
  brokerutil
  api_cas
#  brokeradmin
  RUNTIME DESTINATION ${CUBRID_BINDIR} COMPONENT Application
  LIBRARY DESTINATION ${CUBRID_LIBDIR} COMPONENT Library
  PUBLIC_HEADER DESTINATION ${CUBRID_INCLUDEDIR} COMPONENT Header
  )


# TODO: compat uc script?

# install pdb files for debugging on windows
if(WIN32)
  install(DIRECTORY
    ${CMAKE_CURRENT_BINARY_DIR}/\${CMAKE_INSTALL_CONFIG_NAME}/
    DESTINATION ${CUBRID_BINDIR} COMPONENT Debuginfo
    FILES_MATCHING PATTERN "*.pdb"
    )
endif(WIN32)
