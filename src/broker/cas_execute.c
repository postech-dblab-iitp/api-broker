/*
 * Copyright 2008 Search Solution Corporation
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


/*
 * cas_execute.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#include <io.h>
#include <fcntl.h>
#include <process.h>
#else /* WINDOWS */
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#endif /* WINDOWS */
#include <assert.h>

#include "cas.h"
#include "cas_common.h"
#include "cas_execute.h"
#include "cas_network.h"
#include "cas_util.h"
#include "cas_schema_info.h"
#include "cas_log.h"
#include "cas_str_like.h"

#include "broker_filename.h"
#include "cas_sql_log2.h"

#include "intl_support.h"

#include "s62ext.h"

#if defined (SUPPRESS_STRLEN_WARNING)
#define strlen(s1)  ((int) strlen(s1))
#endif /* defined (SUPPRESS_STRLEN_WARNING) */

#define QUERY_BUFFER_MAX                4096

#define FK_INFO_SORT_BY_PKTABLE_NAME	1
#define FK_INFO_SORT_BY_FKTABLE_NAME	2
#define DBLINK_HINT                     "DBLINK"

typedef enum
{
  NONE_TOKENS,
  SQL_STYLE_COMMENT,
  C_STYLE_COMMENT,
  CPP_STYLE_COMMENT,
  SINGLE_QUOTED_STRING,
  DOUBLE_QUOTED_STRING
} STATEMENT_STATUS;

#if !defined(WINDOWS)
#define STRING_APPEND(buffer_p, avail_size_holder, ...) \
  do {                                                          \
    if (avail_size_holder > 0) {                                \
      int n = snprintf (buffer_p, avail_size_holder, __VA_ARGS__);	\
      if (n > 0)        {                                       \
        if (n < avail_size_holder) {                            \
          buffer_p += n; avail_size_holder -= n;                \
        } else {                                                \
          buffer_p += (avail_size_holder - 1); 			\
	  avail_size_holder = 0; 				\
        }                                                       \
      }                                                         \
    }								\
  } while (0)
#else /* !WINDOWS */
#define STRING_APPEND(buffer_p, avail_size_holder, ...) \
  do {                                                          \
    if (avail_size_holder > 0) {                                \
      int n = _snprintf (buffer_p, avail_size_holder, __VA_ARGS__);	\
      if (n < 0 || n >= avail_size_holder) {                    \
        buffer_p += (avail_size_holder - 1);                    \
        avail_size_holder = 0;                                  \
        *buffer_p = '\0';                                       \
      } else {                                                  \
        buffer_p += n; avail_size_holder -= n;                  \
      }                                                         \
    }                                                           \
  } while (0)
#endif /* !WINDOWS */

#define IS_NULL_CAS_TYPE(cas_type) ((cas_type) == CCI_U_TYPE_NULL)

/* borrowed from optimizer.h: OPT_LEVEL, OPTIMIZATION_ENABLED,
 *                            PLAN_DUMP_ENABLED, SIMPLE_DUMP,
 *                            DETAILED_DUMP
 */
#define CHK_OPT_LEVEL(level)                ((level) & 0xff)
#define CHK_OPTIMIZATION_ENABLED(level)     (CHK_OPT_LEVEL(level) != 0)
#define CHK_PLAN_DUMP_ENABLED(level)        ((level) >= 0x100)
#define CHK_SIMPLE_DUMP(level)              ((level) & 0x100)
#define CHK_DETAILED_DUMP(level)            ((level) & 0x200)
#define CHK_OPTIMIZATION_LEVEL_VALID(level) \
	  (CHK_OPTIMIZATION_ENABLED(level) \
	   || CHK_PLAN_DUMP_ENABLED(level) \
           || (level == 0))


typedef int (*T_FETCH_FUNC) (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);

typedef struct t_priv_table T_PRIV_TABLE;
struct t_priv_table
{
  char *class_name;
  char priv;
  char grant;
};

typedef struct t_class_table T_CLASS_TABLE;
struct t_class_table
{
  char *class_name;
  short class_type;
};

typedef struct t_attr_table T_ATTR_TABLE;
struct t_attr_table
{
  const char *class_name;
  const char *attr_name;
  const char *source_class;
  int precision;
  short scale;
  short attr_order;
  void *default_val;
  unsigned char domain;
  char indexed;
  char non_null;
  char shared;
  char unique;
  char set_domain;
  char is_key;
  const char *comment;
};

/*
  fetch_xxx prototype:
  fetch_xxx(T_SRV_HANDLE *, int cursor_pos, int fetch_count, char fetch_flag,
	    int result_set_idx, T_NET_BUF *);
*/
static int fetch_result (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_class (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_attribute (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_method (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_methfile (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_constraint (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_trigger (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_privilege (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_foreign_keys (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_not_supported (T_SRV_HANDLE *, int, int, char, int, T_NET_BUF *, T_REQ_INFO *);
static void add_res_data_bytes (T_NET_BUF * net_buf, const char *str, int size, unsigned char ext_type, int *net_size);
static void add_res_data_string (T_NET_BUF * net_buf, const char *str, int size, unsigned char ext_type,
				 unsigned char charset, int *net_size);
static void add_res_data_string_safe (T_NET_BUF * net_buf, const char *str, unsigned char ext_type,
				      unsigned char charset, int *net_size);
static void add_res_data_int (T_NET_BUF * net_buf, int value, unsigned char ext_type, int *net_size);
static void add_res_data_bigint (T_NET_BUF * net_buf, DB_BIGINT value, unsigned char ext_type, int *net_size);
static void add_res_data_short (T_NET_BUF * net_buf, short value, unsigned char ext_type, int *net_size);
static void add_res_data_float (T_NET_BUF * net_buf, float value, unsigned char ext_type, int *net_size);
static void add_res_data_double (T_NET_BUF * net_buf, double value, unsigned char ext_type, int *net_size);
static void add_res_data_timestamp (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss,
				    unsigned char ext_type, int *net_size);
static void add_res_data_timestamptz (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss,
				      char *tz_str, unsigned char ext_type, int *net_size);
static void add_res_data_datetime (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss,
				   short ms, unsigned char ext_type, int *net_size);
static void add_res_data_datetimetz (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss,
				     short ms, char *tz_str, unsigned char ext_type, int *net_size);
static void add_res_data_time (T_NET_BUF * net_buf, short hh, short mm, short ss, unsigned char ext_type,
			       int *net_size);
static void add_res_data_date (T_NET_BUF * net_buf, short yr, short mon, short day, unsigned char ext_type,
			       int *net_size);
static void add_res_data_object (T_NET_BUF * net_buf, T_OBJECT * obj, unsigned char ext_type, int *net_size);
static void add_res_data_lob_handle (T_NET_BUF * net_buf, T_LOB_HANDLE * lob, unsigned char ext_type, int *net_size);
static int get_num_markers (char *stmt, char **return_stmt);
static char *consume_tokens (char *stmt, STATEMENT_STATUS stmt_status);
static char get_stmt_type (char *stmt);
static int prepare_column_list_info_set (char prepare_flag, T_QUERY_RESULT * q_result, T_NET_BUF * net_buf);
static int execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf, T_BROKER_VERSION client_version,
			     char exec_flag);
static int sch_class_info (T_NET_BUF * net_buf, char *class_name, char pattern_flag, char flag, void **result);
static int sch_attr_info (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pattern_flag, char flag,
			  void **result);
static int sch_attr_with_synonym_info (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pattern_flag,
				       char flag, T_SRV_HANDLE *);
static int sch_queryspec (T_NET_BUF * net_buf, char *class_name, T_SRV_HANDLE *);
static void sch_method_info (T_NET_BUF * net_buf, char *class_name, char flag, void **result);
static void sch_methfile_info (T_NET_BUF * net_buf, char *class_name, void **result);
static int sch_superclass (T_NET_BUF * net_buf, char *class_name, char flag, T_SRV_HANDLE * srv_handle);
static void sch_constraint (T_NET_BUF * net_buf, char *class_name, void **result);
static void sch_trigger (T_NET_BUF * net_buf, char *class_name, char flag, void **result);
static int sch_class_priv (T_NET_BUF * net_buf, char *class_name, char pat_flag, T_SRV_HANDLE * srv_handle);
static int sch_attr_priv (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pat_flag,
			  T_SRV_HANDLE * srv_handle);
static int sch_direct_super_class (T_NET_BUF * net_buf, char *class_name, int pattern_flag, T_SRV_HANDLE * srv_handle);
static int sch_imported_keys (T_NET_BUF * net_buf, char *class_name, void **result);
static int sch_exported_keys_or_cross_reference (T_NET_BUF * net_buf, bool find_cross_ref, char *pktable_name,
						 char *fktable_name, void **result);
static int set_priv_table (unsigned int class_priv, char *name, T_PRIV_TABLE * priv_table, int index);
static int sch_query_execute (T_SRV_HANDLE * srv_handle, char *sql_stmt, T_NET_BUF * net_buf);
static int sch_primary_key (T_NET_BUF * net_buf, char *class_name, T_SRV_HANDLE * srv_handle);
static short constraint_dbtype_to_castype (int db_const_type);

static T_PREPARE_CALL_INFO *make_prepare_call_info (int num_args, int is_first_out);
static void prepare_call_info_dbval_clear (T_PREPARE_CALL_INFO * call_info);
static int fetch_call (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
#define check_class_chn(s) 0

static bool has_stmt_result_set (char stmt_type);
static bool check_auto_commit_after_getting_result (T_SRV_HANDLE * srv_handle);
static char *get_backslash_escape_string (void);

static void update_query_execution_count (T_APPL_SERVER_INFO * as_info_p, char stmt_type);
static bool need_reconnect_on_rctime (void);
static void report_abnormal_host_status (int err_code);

static unsigned char set_extended_cas_type (T_CCI_U_TYPE u_set_type, DB_TYPE db_type);
static short encode_ext_type_to_short (unsigned char cas_type);
static int ux_get_generated_keys_server_insert (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf);
static int ux_get_generated_keys_client_insert (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf);

static bool do_commit_after_execute (const t_srv_handle & server_handle);
static int recompile_statement (T_SRV_HANDLE * srv_handle);

static int bind_from_netval (S62_STATEMENT * stmt_id, int idx, void *net_type, void *net_value, T_NET_BUF * net_buf,
			     char desired_type);
static int value_to_netbuf (S62_RESULTSET * resultset, T_NET_BUF * net_buf, int idx, int max_col_size,
			    char column_type_flag);

static s62_string hugeint_to_string(s62_hugeint *hugeint);
static void time_to_timestamp(int64_t time, int *year, int *month, int *day, int *hh, int *mm, int *ss);
static void days_to_date(s62_date days, int *year, int *month, int *day);

static char cas_u_type[] = { 0,	/* 0 */
  CCI_U_TYPE_INT,		/* 1 */
  CCI_U_TYPE_FLOAT,		/* 2 */
  CCI_U_TYPE_DOUBLE,		/* 3 */
  CCI_U_TYPE_STRING,		/* 4 */
  CCI_U_TYPE_OBJECT,		/* 5 */
  CCI_U_TYPE_SET,		/* 6 */
  CCI_U_TYPE_MULTISET,		/* 7 */
  CCI_U_TYPE_SEQUENCE,		/* 8 */
  0,				/* 9 */
  CCI_U_TYPE_TIME,		/* 10 */
  CCI_U_TYPE_TIMESTAMP,		/* 11 */
  CCI_U_TYPE_DATE,		/* 12 */
  CCI_U_TYPE_MONETARY,		/* 13 */
  0, 0, 0, 0,			/* 14 - 17 */
  CCI_U_TYPE_SHORT,		/* 18 */
  0, 0, 0,			/* 19 - 21 */
  CCI_U_TYPE_NUMERIC,		/* 22 */
  CCI_U_TYPE_BIT,		/* 23 */
  CCI_U_TYPE_VARBIT,		/* 24 */
  CCI_U_TYPE_CHAR,		/* 25 */
  CCI_U_TYPE_NCHAR,		/* 26 */
  CCI_U_TYPE_VARNCHAR,		/* 27 */
  CCI_U_TYPE_RESULTSET,		/* 28 */
  0, 0,				/* 29 - 30 */
  CCI_U_TYPE_BIGINT,		/* 31 */
  CCI_U_TYPE_DATETIME,		/* 32 */
  CCI_U_TYPE_BLOB,		/* 33 */
  CCI_U_TYPE_CLOB,		/* 34 */
  CCI_U_TYPE_ENUM,		/* 35 */
  CCI_U_TYPE_TIMESTAMPTZ,	/* 36 */
  CCI_U_TYPE_TIMESTAMPLTZ,	/* 37 */
  CCI_U_TYPE_DATETIMETZ,	/* 38 */
  CCI_U_TYPE_DATETIMELTZ,	/* 39 */
  CCI_U_TYPE_JSON,		/* 40 */
  CCI_U_TYPE_HUGEINT, /* 41 */
};

#if defined (FOR_API_CAS)
static T_FETCH_FUNC fetch_func[] = {
  fetch_result,			/* query */
  fetch_class,			/* SCH_CLASS */
  fetch_class,			/* SCH_VCLASS */
  fetch_not_supported,		/* SCH_QUERY_SPEC */
  fetch_attribute,		/* SCH_ATTRIBUTE */
  fetch_not_supported,		/* SCH_CLASS_ATTRIBUTE */
  fetch_not_supported,		/* SCH_METHOD */
  fetch_not_supported,		/* SCH_CLASS_METHOD */
  fetch_not_supported,		/* SCH_METHOD_FILE */
  fetch_not_supported,		/* SCH_SUPERCLASS */
  fetch_not_supported,		/* SCH_SUBCLASS */
  fetch_not_supported,		/* SCH_CONSTRAINT */
  fetch_not_supported,		/* SCH_TRIGGER */
  fetch_not_supported,		/* SCH_CLASS_PRIVILEGE */
  fetch_not_supported,		/* SCH_ATTR_PRIVILEGE */
  fetch_not_supported,		/* SCH_DIRECT_SUPER_CLASS */
  fetch_not_supported,		/* SCH_PRIMARY_KEY */
  fetch_not_supported,		/* SCH_IMPORTED_KEYS */
  fetch_not_supported,		/* SCH_EXPORTED_KEYS */
};
#else
static T_FETCH_FUNC fetch_func[] = {
  fetch_result,			/* query */
  fetch_result,			/* SCH_CLASS */
  fetch_result,			/* SCH_VCLASS */
  fetch_result,			/* SCH_QUERY_SPEC */
  fetch_attribute,		/* SCH_ATTRIBUTE */
  fetch_attribute,		/* SCH_CLASS_ATTRIBUTE */
  fetch_method,			/* SCH_METHOD */
  fetch_method,			/* SCH_CLASS_METHOD */
  fetch_methfile,		/* SCH_METHOD_FILE */
  fetch_class,			/* SCH_SUPERCLASS */
  fetch_class,			/* SCH_SUBCLASS */
  fetch_constraint,		/* SCH_CONSTRAINT */
  fetch_trigger,		/* SCH_TRIGGER */
  fetch_privilege,		/* SCH_CLASS_PRIVILEGE */
  fetch_privilege,		/* SCH_ATTR_PRIVILEGE */
  fetch_result,			/* SCH_DIRECT_SUPER_CLASS */
  fetch_result,			/* SCH_PRIMARY_KEY */
  fetch_foreign_keys,		/* SCH_IMPORTED_KEYS */
  fetch_foreign_keys,		/* SCH_EXPORTED_KEYS */
  fetch_foreign_keys,		/* SCH_CROSS_REFERENCE */
  fetch_attribute,		/* SCH_ATTR_WITH_SYNONYM */
};
#endif

static char database_name[MAX_HA_DBINFO_LENGTH] = "";
static char database_user[SRV_CON_DBUSER_SIZE] = "";
static char database_passwd[SRV_CON_DBPASSWD_SIZE] = "";
static char cas_db_sys_param[128] = "";
static int saved_Optimization_level = -1;

/*****************************
  move from cas_log.c
 *****************************/
/* log error handler related fields */
typedef struct cas_error_log_handle_context_s CAS_ERROR_LOG_HANDLE_CONTEXT;
struct cas_error_log_handle_context_s
{
  unsigned int from;
  unsigned int to;
};
static CAS_ERROR_LOG_HANDLE_CONTEXT *cas_EHCTX = NULL;

int
ux_check_connection (void)
{
  cas_log_write (0, true, "--ux_check_connection");

  if (ux_is_database_connected ())
    {
      // if there is function to check connecting at tg++, we need to call this function
      return 0;
    }
  else
    {
      return -1;
    }

  return 0;
}

int
ux_database_connect (char *db_name, char *db_user, char *db_passwd, char **db_err_msg)
{
  char *p;
  int err_code;
  char workspace[500];
  char *errstr;

  cas_log_write (0, true, "--ux_database_connect: dbname(%s), db_user(%s), db_passwd(%s)", db_name, db_user, db_passwd);

  as_info->force_reconnect = false;

  if (db_name == NULL || db_name[0] == '\0')
    {
      return ERROR_INFO_SET (-1, CAS_ERROR_INDICATOR);
    }

  if (database_name[0] == '\0' || strcmp (database_name, db_name) != 0)
    {
      if (database_name[0] != '\0')
	{
	  ux_database_shutdown ();
	}

      if ((err_code = s62_get_workspace (db_name, (char *) workspace)) < 0)
	{
	  cas_log_write (0, true, "can't get workspace with %s [%d]", db_name, err_code);;
	  return ERROR_INFO_SET (-2, CAS_ERROR_INDICATOR);
	}

      cas_log_write (0, true, "-- s62_connect (%s) -- before call\n", workspace);
      if ((err_code = s62_connect (workspace)) < 0)
	{
	  char *errstr;
	  cas_log_write (0, true, "can't connect db with %s [%d]", workspace, s62_get_last_error (&errstr));;
	  goto connect_error;
	}
      cas_log_write (0, true, "-- s62_connect (%s) -- after call\n", workspace);

      p = strchr (db_name, '@');
      if (p)
	{
	  *p = '\0';
	  strncpy (as_info->database_name, db_name, sizeof (as_info->database_name) - 1);
	  *p = (char) '@';
	  strncpy (as_info->database_host, p + 1, sizeof (as_info->database_host) - 1);
	}
      else
	{
	  strncpy (as_info->database_name, db_name, sizeof (as_info->database_name) - 1);
	  strncpy (as_info->database_host, "localhost", sizeof (as_info->database_host) - 1);
	}
      as_info->last_connect_time = time (NULL);

      strncpy (database_name, db_name, sizeof (database_name) - 1);
      strncpy (database_user, db_user, sizeof (database_user) - 1);
      strncpy (database_passwd, db_passwd, sizeof (database_passwd) - 1);
    }
  else if (shm_appl->cache_user_info == OFF
	   || strcmp (database_user, db_user) != 0 || strcmp (database_passwd, db_passwd) != 0)
    {
      // relogin without connecting
      cas_log_write (0, true, "-- already be connected, but don't need to connect");
      strncpy (database_user, db_user, sizeof (database_user) - 1);
      strncpy (database_passwd, db_passwd, sizeof (database_passwd) - 1);
    }
  else
    {
      cas_log_write (0, true, "-- already be connected, but don't need to connect");
      // already be connected, but we need to check whether connecting to tg++
    }

  return 0;

connect_error:

  return ERROR_INFO_SET_WITH_MSG (s62_get_last_error (&errstr), DBMS_ERROR_INDICATOR, "ux_database_connect error !!");
}

int
ux_is_database_connected (void)
{
  int is_connected = 0;

  cas_log_write (0, true, "--ux_is_database_connected");
  if (s62_is_connected () == S62_CONNECTED)
    {
      is_connected = 1;
    }

  return (is_connected);
}

void
ux_database_shutdown ()
{
  // call function for disconnecting from tg++

  cas_log_write (0, true, "--ux_database_shutdown");

  s62_disconnect ();

  as_info->database_name[0] = '\0';
  as_info->database_host[0] = '\0';
  as_info->database_user[0] = '\0';
  as_info->database_passwd[0] = '\0';
  as_info->last_connect_time = 0;

  database_name[0] = '\0';
  database_user[0] = '\0';
  database_passwd[0] = '\0';
}

int
ux_prepare (char *sql_stmt, int flag, char auto_commit_mode, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
	    unsigned int query_seq_num)
{
  S62_STATEMENT *stmt_id = NULL;
  T_SRV_HANDLE *srv_handle = NULL;
  T_QUERY_RESULT *q_result = NULL;
  int srv_h_id = -1;
  int num_markers = 0;
  int stmt_type;
  int err_code;
  int result_cache_lifetime;
  char *errstr;

  cas_log_write (0, true, "--ux_prepare (%s)", sql_stmt);

  srv_h_id = hm_new_srv_handle (&srv_handle, query_seq_num);

  if (srv_h_id < 0)
    {
      err_code = srv_h_id;
      goto prepare_error;
    }
  srv_handle->schema_type = -1;
  srv_handle->auto_commit_mode = auto_commit_mode;

  num_markers = get_num_markers (sql_stmt, &(srv_handle->sql_stmt));
  if (srv_handle->sql_stmt == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto prepare_error;
    }

  sql_stmt = srv_handle->sql_stmt;

  if (flag & CCI_PREPARE_CALL
      || flag & CCI_PREPARE_UPDATABLE || flag & CCI_PREPARE_INCLUDE_OID || flag & CCI_PREPARE_XASL_CACHE_PINNED)
    {
      err_code = ERROR_INFO_SET (CAS_ER_INVALID_CALL_STMT, CAS_ERROR_INDICATOR);
      goto prepare_error;
    }

  stmt_id = s62_prepare (sql_stmt);
  if (stmt_id == NULL)
    {
      srv_handle->is_prepared = FALSE;
      err_code = ERROR_INFO_SET (s62_get_last_error (&errstr), DBMS_ERROR_INDICATOR);
      goto prepare_error;
    }
  else
    {
      stmt_type = S62_STMT_MATCH;
      srv_handle->stmt_type = stmt_type;
      srv_handle->is_prepared = TRUE;
      srv_handle->query_info_flag = TRUE;
    }

prepare_result_set:
  srv_handle->stmt_id = stmt_id;
  srv_handle->num_markers = num_markers;
  srv_handle->prepare_flag = flag;

  net_buf_cp_int (net_buf, srv_h_id, NULL);

  result_cache_lifetime = -1;
  net_buf_cp_int (net_buf, result_cache_lifetime, NULL);

  net_buf_cp_byte (net_buf, stmt_type);
  net_buf_cp_int (net_buf, num_markers, NULL);

  q_result = (T_QUERY_RESULT *) malloc (sizeof (T_QUERY_RESULT));
  if (q_result == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto prepare_error;
    }

  hm_qresult_clear (q_result);

  q_result->stmt_type = stmt_type;
  q_result->stmt_id = stmt_id;

  err_code = prepare_column_list_info_set (flag, q_result, net_buf);
  if (err_code < 0)
    {
      FREE_MEM (q_result);
      goto prepare_error;
    }

  srv_handle->session = NULL;
  srv_handle->q_result = q_result;
  srv_handle->num_q_result = 1;
  srv_handle->cur_result = NULL;
  srv_handle->cur_result_index = 0;
  srv_handle->is_holdable = false;
  srv_handle->use_plan_cache = false;
  srv_handle->use_query_cache = false;

  return srv_h_id;

prepare_error:
  NET_BUF_ERR_SET (net_buf);

  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOROLLBACK;
    }

  errors_in_transaction++;

  if (srv_handle)
    {
      hm_srv_handle_free (srv_h_id);
    }

  if (q_result != NULL)
    {
      free (q_result);
      q_result = NULL;
    }

  return err_code;
}

int
ux_end_tran (int tran_type, bool reset_con_status)
{
  cas_log_write (0, true, "--ux_end_tran (%d:%s)", tran_type, reset_con_status == 0 ? "false" : "true");
  return 0;
}

int
ux_get_row_count (T_NET_BUF * net_buf)
{
  cas_log_write (0, true, "--ux_get_row_count");
  return 0;
}

int
ux_get_last_insert_id (T_NET_BUF * net_buf)
{
  cas_log_write (0, true, "--ux_get_last_insert_id");
  return 0;
}

int
ux_execute (T_SRV_HANDLE * srv_handle, char flag, int max_col_size, int max_row, int argc, void **argv,
	    T_NET_BUF * net_buf, T_REQ_INFO * req_info, CACHE_TIME * clt_cache_time, int *clt_cache_reusable)
{
  int err_code;
  int num_bind = 0;
  char stmt_type;
  S62_STATEMENT *stmt_id = srv_handle->stmt_id;
  S62_RESULTSET *resultset = (S62_RESULTSET *) NULL;
  int i, type_idx, val_idx;
  int num_result = -1;
  char *errstr;

  cas_log_write (0, true, "--ux_execute");
  hm_qresult_end (srv_handle, FALSE);

// we need to add code for requesting flag & CCI_EXEC_QUERY_INFO
  if (srv_handle->is_prepared == FALSE || stmt_id == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      cas_log_write (0, true, "can't execute statement not prepared");
      goto execute_error;
    }

  num_bind = srv_handle->num_markers;

  if (num_bind > 0)
    {
      if (num_bind != (argc / 2))
	{
	  err_code = ERROR_INFO_SET (CAS_ER_NUM_BIND, CAS_ERROR_INDICATOR);
	  goto execute_error;
	}

      for (i = 0; i < num_bind; i++)
	{
	  type_idx = 2 * i;
	  val_idx = 2 * i + 1;
	  err_code = bind_from_netval (stmt_id, i, argv[type_idx], argv[val_idx], net_buf, DB_TYPE_NULL);
	  if (err_code < 0)
	    {
	      goto execute_error;
	    }
	}
    }

  srv_handle->is_from_current_transaction = true;
  hm_set_current_srv_handle (srv_handle->id);

  num_result = s62_execute (stmt_id, &resultset);
  if (num_result < 0)
    {
      err_code = ERROR_INFO_SET (s62_get_last_error (&errstr), DBMS_ERROR_INDICATOR);
      goto execute_error;
    }
  stmt_type = srv_handle->stmt_type;
  hm_set_current_srv_handle (-1);

  update_query_execution_count (as_info, stmt_type);

  net_buf_cp_int (net_buf, num_result, NULL);

  srv_handle->max_col_size = max_col_size;
  srv_handle->num_q_result = 1;
  srv_handle->q_result->result = resultset;
  srv_handle->q_result->tuple_count = num_result;
  srv_handle->cur_result = (void *) srv_handle->q_result;
  srv_handle->cur_result_index = 1;
  srv_handle->max_row = max_row;

  if (has_stmt_result_set (srv_handle->q_result->stmt_type) == true)
    {
      srv_handle->has_result_set = true;
      srv_handle->is_holdable = false;
    }

  req_info->need_auto_commit = TRAN_AUTOCOMMIT;

  // clt_cache_reusable
  net_buf_cp_byte (net_buf, 0);

  // num_q_result
  net_buf_cp_int (net_buf, srv_handle->num_q_result, NULL);	// only if num_q_result == 1

  // stmt_type
  net_buf_cp_byte (net_buf, stmt_type);

  // tuple_count
  net_buf_cp_int (net_buf, num_result, NULL);

  // insert oid
  {
    T_OBJECT ins_oid;
    memset (&ins_oid, 0, sizeof (T_OBJECT));
    net_buf_cp_object (net_buf, &ins_oid);
  }

  // cache time sec & usec
  net_buf_cp_int (net_buf, 0, NULL);
  net_buf_cp_int (net_buf, 0, NULL);

  // include_column_info
  net_buf_cp_byte (net_buf, 1);
  net_buf_cp_int (net_buf, -1, NULL);
  net_buf_cp_byte (net_buf, stmt_type);
  net_buf_cp_int (net_buf, srv_handle->num_markers, NULL);
  err_code = prepare_column_list_info_set (srv_handle->prepare_flag, srv_handle->q_result, net_buf);
  if (err_code != 0)
    {
      goto execute_error;
    }

  // shard id
  net_buf_cp_int (net_buf, -2, NULL);

  return err_code;

execute_error:
  NET_BUF_ERR_SET (net_buf);

  if (srv_handle->auto_commit_mode)
    {
      req_info->need_auto_commit = TRAN_AUTOROLLBACK;
    }

  errors_in_transaction++;

  return err_code;
}

int
ux_execute_all (T_SRV_HANDLE * srv_handle, char flag, int max_col_size, int max_row, int argc, void **argv,
		T_NET_BUF * net_buf, T_REQ_INFO * req_info, CACHE_TIME * clt_cache_time, int *clt_cache_reusable)
{
  cas_log_write (0, true, "--ux_execute_all");
  return 0;
}

int
ux_next_result (T_SRV_HANDLE * srv_handle, char flag, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  cas_log_write (0, true, "--ux_next_result");
  return 0;
}

void
ux_set_cas_change_mode (int mode, T_NET_BUF * net_buf)
{
  int prev_mode;

  prev_mode = as_info->cas_change_mode;
  as_info->cas_change_mode = mode;

  net_buf_cp_int (net_buf, 0, NULL);	/* result code */
  net_buf_cp_int (net_buf, prev_mode, NULL);	/* result msg */

  cas_log_write (0, true, "--ux_set_cas_change_mode");
}

int
ux_fetch (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_index,
	  T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int err_code;
  int fetch_func_index;

  cas_log_write (0, true, "--ux_fetch");

  if (srv_handle == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);

      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "fetch %s%d", "error:", err_info.err_number);

      goto fetch_error;
    }

  if (srv_handle->schema_type < 0)
    {
      fetch_func_index = 0;
    }
  else if (srv_handle->schema_type >= CCI_SCH_FIRST && srv_handle->schema_type <= CCI_SCH_LAST)
    {
      fetch_func_index = srv_handle->schema_type;
    }
  else
    {
      err_code = ERROR_INFO_SET (CAS_ER_SCHEMA_TYPE, CAS_ERROR_INDICATOR);
      goto fetch_error;
    }

  net_buf_cp_int (net_buf, 0, NULL);	/* result code */

  if (fetch_count <= 0)
    {
      fetch_count = 100;
    }

  err_code =
    (*(fetch_func[fetch_func_index])) (srv_handle, cursor_pos, fetch_count, fetch_flag, result_set_index, net_buf,
				       req_info);

  if (err_code < 0)
    {
      goto fetch_error;
    }

  return 0;

fetch_error:
  NET_BUF_ERR_SET (net_buf);

  errors_in_transaction++;
  return err_code;
}

int
ux_cursor (int srv_h_id, int offset, int origin, T_NET_BUF * net_buf)
{
  cas_log_write (0, true, "--ux_cursor");
  return 0;
}

void
ux_cursor_close (T_SRV_HANDLE * srv_handle)
{
  int idx = 0;

  cas_log_write (0, true, "--ux_cursor_close");

  if (srv_handle == NULL)
    {
      return;
    }

  idx = srv_handle->cur_result_index - 1;

  if (idx < 0)
    {
      return;
    }

  s62_close_resultset ((S62_RESULTSET *) srv_handle->q_result[idx].result);
  srv_handle->q_result[idx].result = NULL;
}

int
ux_get_db_version (T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  s62_version p;

  p = s62_get_version_ex ();

  cas_log_write (0, true, "--ux_get_db_version : %s", p);
  net_buf_cp_int (net_buf, 0, NULL);
  if (p == NULL)
    {
      net_buf_cp_byte (net_buf, '\0');
    }
  else
    {
      net_buf_cp_str (net_buf, p, strlen (p) + 1);
    }

  return 0;
}

int
ux_get_class_num_objs (char *class_name, int flag, T_NET_BUF * net_buf)
{
  cas_log_write (0, true, "--ux_get_class_num_objs");
  return 0;
}

int
make_bind_value (int num_bind, int argc, void **argv, void **ret_val, T_NET_BUF * net_buf, char desired_type)
{
  cas_log_write (0, true, "--make_bind_value");
  return 0;
}

int
ux_get_attr_type_str (char *class_name, char *attr_name, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  cas_log_write (0, true, "--ux_get_attr_type_str");
  return 0;
}

int
ux_get_query_info (int srv_h_id, char *sql_stmt, T_NET_BUF * net_buf)
{
  T_SRV_HANDLE *srv_handle;
  int err_code;
  char *errstr;
  int tmp_id = -1;

  cas_log_write (0, true, "--ux_get_query_info");

  srv_handle = hm_find_srv_handle (srv_h_id);

  if (srv_handle == NULL && sql_stmt == NULL)
    {
      net_buf_cp_byte (net_buf, '\0');
      goto end;
    }
  else if (srv_handle == NULL && sql_stmt != NULL)
    {
      S62_STATEMENT *stmt_id = NULL;
      int err_code;
      char *errstr;

      tmp_id = hm_new_srv_handle (&srv_handle, query_seq_num_next_value ());
      if (tmp_id < 0)
	{
	  net_buf_cp_byte (net_buf, '\0');
	  goto end;
	}

      get_num_markers (sql_stmt, &(srv_handle->sql_stmt));
      if (srv_handle->sql_stmt == NULL)
	{
	  err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
	  NET_BUF_ERR_SET (net_buf);
	  goto end;
	}

      sql_stmt = srv_handle->sql_stmt;

      stmt_id = s62_prepare (sql_stmt);
      if (stmt_id == NULL)
	{
	  err_code = ERROR_INFO_SET (s62_get_last_error (&errstr), DBMS_ERROR_INDICATOR);
	  NET_BUF_ERR_SET (net_buf);
	  goto end;
	}
      srv_handle->query_info_flag = TRUE;
      srv_handle->stmt_id = stmt_id;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  if (srv_handle->stmt_id->plan != NULL)
    {
      net_buf_cp_str (net_buf, srv_handle->stmt_id->plan, strlen (srv_handle->stmt_id->plan));
    }
  net_buf_cp_byte (net_buf, '\0');

end:
  if (tmp_id != -1)
    {
      hm_srv_handle_free (tmp_id);
    }

  return 0;
}

void
ux_free_result (void *res)
{
  cas_log_write (0, true, "--ux_free_result");
  s62_close_resultset ((S62_RESULTSET *) res);
}

char
ux_db_type_to_cas_type (int db_type)
{
  /* todo: T_CCI_U_TYPE duplicates db types. */
  if (db_type < DB_TYPE_FIRST || db_type > DB_TYPE_LAST)
    {
      return CCI_U_TYPE_NULL;
    }

  return (cas_u_type[db_type]);
}

int
ux_schema_info (int schema_type, char *arg1, char *arg2, char flag, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
		unsigned int query_seq_num)
{
  int srv_h_id;
  int err_code = 0;
  T_SRV_HANDLE *srv_handle = NULL;
  T_BROKER_VERSION client_version = req_info->client_version;
  int num_results = 0;

  cas_log_write (0, true, "--ux_schema_info");

  srv_h_id = hm_new_srv_handle (&srv_handle, query_seq_num);
  if (srv_h_id < 0)
    {
      err_code = srv_h_id;
      goto schema_info_error;
    }
  srv_handle->schema_type = schema_type;

  net_buf_cp_int (net_buf, srv_h_id, NULL);	/* result code */

  switch (schema_type)
    {
    case CCI_SCH_CLASS:
      cas_log_write (0, true, "--ux_schema_info table : class [%s], pattern_flag [%c]", arg1, flag);
      err_code = sch_class_info (net_buf, arg1, flag, 0, &(srv_handle->session));
      break;
    case CCI_SCH_ATTRIBUTE:
      cas_log_write (0, true, "--ux_schema_info column");
      err_code = sch_attr_info (net_buf, arg1, arg2, flag, 0, &(srv_handle->session));
      break;
    default:
      err_code = ERROR_INFO_SET (CAS_ER_SCHEMA_TYPE, CAS_ERROR_INDICATOR);
      goto schema_info_error;
    }

  if (err_code < 0)
    {
      goto schema_info_error;
    }

#if 0
  if (schema_type == CCI_SCH_CLASS || schema_type == CCI_SCH_ATTRIBUTE)
    {
      srv_handle->cursor_pos = 0;
    }
  else
    {
#endif
      srv_handle->cur_result = srv_handle->session;
      if (srv_handle->cur_result == NULL)
	{
	  srv_handle->cursor_pos = 0;
	}
      else
	{
	  srv_handle->cursor_pos = 1;
	}
#if 0
    }
#endif

  return srv_h_id;

schema_info_error:
  NET_BUF_ERR_SET (net_buf);
  errors_in_transaction++;
  if (srv_handle)
    {
      hm_srv_handle_free (srv_h_id);
    }

  return err_code;
}

static int
fetch_result (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
	      T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  T_OBJECT tuple_obj;
  int err_code;
  int num_tuple_msg_offset;
  int num_tuple;
  int net_buf_size;
  char fetch_end_flag = 0;
  T_QUERY_RESULT *q_result;
  S62_RESULTSET *result;
  char sensitive_flag = fetch_flag & CCI_FETCH_SENSITIVE;
  int num_properties;
  int i;
  s62_fetch_state ret_fetch = S62_ERROR_RESULT;
  int *type_properties = NULL;
  char *errstr;

  cas_log_write (0, true, "--fetch_result");

  if (result_set_idx <= 0)
    {
      q_result = (T_QUERY_RESULT *) (srv_handle->cur_result);
      if (q_result == NULL)
	{
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
	}
    }
  else
    {
      if (result_set_idx > srv_handle->cur_result_index)
	{
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_RESULT_SET, CAS_ERROR_INDICATOR);
	}
      q_result = srv_handle->q_result + (result_set_idx - 1);
    }

  sensitive_flag = FALSE;

  result = (S62_RESULTSET *) q_result->result;

  if (result == NULL || has_stmt_result_set (q_result->stmt_type) == false)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
    }


  num_properties = s62_get_property_count (result);

  if (num_properties > 0)
    {
      type_properties = (int *) malloc (sizeof (int) * num_properties);
      if (type_properties == NULL)
	{
	  err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
	  goto fetch_error;
	}

      for (i = 0; i < num_properties; i++)
	{
	  *(type_properties + i) = s62_get_property_type (srv_handle->stmt_id, i);
	}
    }

  net_buf_cp_int (net_buf, 0, &num_tuple_msg_offset);
  net_buf_size = NET_BUF_SIZE;
  num_tuple = 0;

  while (CHECK_NET_BUF_SIZE (net_buf, net_buf_size))
    {
      memset ((char *) &tuple_obj, 0, sizeof (T_OBJECT));
      net_buf_cp_int (net_buf, cursor_pos, NULL);
      net_buf_cp_object (net_buf, &tuple_obj);

      ret_fetch = s62_fetch_next (result);
      if (ret_fetch == S62_ERROR_RESULT)
	{
	  err_code = ERROR_INFO_SET (s62_get_last_error (&errstr), DBMS_ERROR_INDICATOR);
	  goto fetch_error;
	}
      else if (ret_fetch == S62_END_OF_RESULT)
	{
	  fetch_end_flag = 1;
	  ux_cursor_close (srv_handle);
	  break;
	}

      for (i = 0; i < num_properties; i++)
	{
	  err_code = value_to_netbuf (result, net_buf, i, srv_handle->max_col_size, *(type_properties + i));
	  if (err_code < 0)
	    {
	      goto fetch_error;
	    }
	}

      num_tuple++;
      cursor_pos++;
    }

  if (type_properties != NULL)
    {
      free (type_properties);
      type_properties = NULL;
    }

  net_buf_cp_byte (net_buf, fetch_end_flag);

  net_buf_overwrite_int (net_buf, num_tuple_msg_offset, num_tuple);

  srv_handle->cursor_pos = cursor_pos;

  return 0;

fetch_error:
  ux_cursor_close (srv_handle);

  if (type_properties != NULL)
    {
      free (type_properties);
      type_properties = NULL;
    }

  return err_code;
}

static int
fetch_class (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
	     T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  T_OBJECT dummy_obj = { 0, 0, 0 };
  S62_METADATA *meta_res;
  int tuple_num_msg_offset;
  char fetch_end_flag = 0;

  cas_log_write (0, true, "--fetch_class");

  cursor_pos = 1;
  net_buf_cp_int (net_buf, 0, &tuple_num_msg_offset);

  for (meta_res = (S62_METADATA *) srv_handle->session; meta_res != NULL; meta_res = meta_res->next)
    {
      net_buf_cp_int (net_buf, cursor_pos, NULL);
      net_buf_cp_object (net_buf, &dummy_obj);

      // 1. NAME (label name)
      add_res_data_string_safe (net_buf, meta_res->label_name, 0, CAS_SCHEMA_DEFAULT_CHARSET, NULL);
      // 2. TYPE (lable type)
      if (meta_res->type == S62_NODE)
	add_res_data_short (net_buf, 2, 0, NULL);
      else if (meta_res->type == S62_EDGE)
	add_res_data_short (net_buf, 1, 0, NULL);
      else
	add_res_data_short (net_buf, meta_res->type, 0, NULL);
      // 3. REMARK (comment)
      add_res_data_string (net_buf, "", 0, 0, CAS_SCHEMA_DEFAULT_CHARSET, NULL);

      cursor_pos++;
    }

  if (meta_res == NULL)
    {
      fetch_end_flag = 1;
    }

  net_buf_cp_byte (net_buf, fetch_end_flag);

  net_buf_overwrite_int (net_buf, tuple_num_msg_offset, cursor_pos - 1);

  return 0;
}

static int
fetch_attribute (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
		 T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  T_OBJECT dummy_obj = { 0, 0, 0 };
  S62_PROPERTY *property_res;
  int tuple_num_msg_offset;
  char fetch_end_flag = 0;
  unsigned char type;

  cas_log_write (0, true, "--fetch_atrribute");

  cursor_pos = 1;
  net_buf_cp_int (net_buf, 0, &tuple_num_msg_offset);

  for (property_res = (S62_PROPERTY *) srv_handle->session; property_res != NULL; property_res = property_res->next)
    {
      net_buf_cp_int (net_buf, cursor_pos, NULL);
      net_buf_cp_object (net_buf, &dummy_obj);

      // 1. ATTR_NAME (name)
      add_res_data_string (net_buf, property_res->property_name, strlen (property_res->property_name), 0,
			   CAS_SCHEMA_DEFAULT_CHARSET, NULL);
      // 2. DOMAIN (lable type)
      type = set_extended_cas_type (CCI_U_TYPE_UNKNOWN, s62type_to_dbtype (property_res->property_type));
      add_res_data_short (net_buf, encode_ext_type_to_short (type), 0, NULL);
      // 3. SCALE
      add_res_data_short (net_buf, (short) property_res->scale, 0, NULL);
      // 4. PRECISION
      add_res_data_int (net_buf, property_res->precision, 0, NULL);
      // 5. INDEXED
      add_res_data_short (net_buf, 0, 0, NULL);
      // 6. NON_NULL
      add_res_data_short (net_buf, 0, 0, NULL);
      // 7. SHARED
      add_res_data_short (net_buf, 0, 0, NULL);
      // 8. UNIQUE
      add_res_data_short (net_buf, 0, 0, NULL);
      // 9. DEFAULT
      add_res_data_string (net_buf, (char *) "", 0, 0, CAS_SCHEMA_DEFAULT_CHARSET, NULL);
      // 10.ORDER
      add_res_data_int (net_buf, property_res->order, 0, NULL);
      // 11.CLASS_NAME
      add_res_data_string (net_buf, property_res->label_name, strlen (property_res->label_name), 0,
			   CAS_SCHEMA_DEFAULT_CHARSET, NULL);
      // 12.SOURCE_CLASS
      add_res_data_string (net_buf, "", 0, 0, CAS_SCHEMA_DEFAULT_CHARSET, NULL);
      // 13.IS_KEY
      add_res_data_short (net_buf, 0, 0, NULL);
      // 14.REMARK (comment)
      add_res_data_string (net_buf, "", 0, 0, CAS_SCHEMA_DEFAULT_CHARSET, NULL);

      cursor_pos++;
    }

  if (property_res == NULL)
    {
      fetch_end_flag = 1;
    }

  net_buf_cp_byte (net_buf, fetch_end_flag);

  net_buf_overwrite_int (net_buf, tuple_num_msg_offset, cursor_pos - 1);

  return 0;
}


static int
fetch_not_supported (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
		     T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  cas_log_write (0, true, "--fetch_not_supported");
  return ERROR_INFO_SET (CAS_ER_NOT_IMPLEMENTED, CAS_ERROR_INDICATOR);
}

static void
add_res_data_bytes (T_NET_BUF * net_buf, const char *str, int size, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + size, NULL);	/* type */
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
    }
  else
    {
      net_buf_cp_int (net_buf, size, NULL);
    }

  /* do not append NULL terminator */
  net_buf_cp_str (net_buf, str, size);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + size;
    }
}

static void
add_res_data_string (T_NET_BUF * net_buf, const char *str, int size, unsigned char ext_type, unsigned char charset,
		     int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + size + 1, NULL);	/* type, NULL terminator */
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, charset);
    }
  else
    {
      net_buf_cp_int (net_buf, size + 1, NULL);	/* NULL terminator */
    }

  net_buf_cp_str (net_buf, str, size);
  net_buf_cp_byte (net_buf, '\0');

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + size + NET_SIZE_BYTE;
    }
}

static void
add_res_data_string_safe (T_NET_BUF * net_buf, const char *str, unsigned char ext_type, unsigned char charset,
			  int *net_size)
{
  if (str != NULL)
    {
      add_res_data_string (net_buf, str, strlen (str), ext_type, charset, net_size);
    }
  else
    {
      add_res_data_string (net_buf, "", 0, ext_type, charset, net_size);
    }
}

static void
add_res_data_int (T_NET_BUF * net_buf, int value, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_INT, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
      net_buf_cp_int (net_buf, value, NULL);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_INT, NULL);
      net_buf_cp_int (net_buf, value, NULL);
    }

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_INT;
    }
}

static void
add_res_data_bigint (T_NET_BUF * net_buf, DB_BIGINT value, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_BIGINT, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
      net_buf_cp_bigint (net_buf, value, NULL);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_BIGINT, NULL);
      net_buf_cp_bigint (net_buf, value, NULL);
    }

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_BIGINT;
    }
}

static void
add_res_data_short (T_NET_BUF * net_buf, short value, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_SHORT, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
      net_buf_cp_short (net_buf, value);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_SHORT, NULL);
      net_buf_cp_short (net_buf, value);
    }

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_SHORT;
    }
}

static void
add_res_data_float (T_NET_BUF * net_buf, float value, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_FLOAT, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
      net_buf_cp_float (net_buf, value);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_FLOAT, NULL);
      net_buf_cp_float (net_buf, value);
    }

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_FLOAT;
    }
}

static void
add_res_data_double (T_NET_BUF * net_buf, double value, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_DOUBLE, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
      net_buf_cp_double (net_buf, value);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_DOUBLE, NULL);
      net_buf_cp_double (net_buf, value);
    }

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_DOUBLE;
    }
}

static void
add_res_data_timestamp (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss,
			unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_TIMESTAMP, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_TIMESTAMP, NULL);
    }

  net_buf_cp_short (net_buf, yr);
  net_buf_cp_short (net_buf, mon);
  net_buf_cp_short (net_buf, day);
  net_buf_cp_short (net_buf, hh);
  net_buf_cp_short (net_buf, mm);
  net_buf_cp_short (net_buf, ss);

  if (net_size)
    {
      *net_size = (NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_TIMESTAMP);
    }
}

static void
add_res_data_datetime (T_NET_BUF * net_buf, short yr, short mon, short day, short hh, short mm, short ss, short ms,
		       unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_DATETIME, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_DATETIME, NULL);
    }

  net_buf_cp_short (net_buf, yr);
  net_buf_cp_short (net_buf, mon);
  net_buf_cp_short (net_buf, day);
  net_buf_cp_short (net_buf, hh);
  net_buf_cp_short (net_buf, mm);
  net_buf_cp_short (net_buf, ss);
  net_buf_cp_short (net_buf, ms);

  if (net_size)
    {
      *net_size = (NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_DATETIME);
    }
}

static void
add_res_data_time (T_NET_BUF * net_buf, short hh, short mm, short ss, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_TIME, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_TIME, NULL);
    }

  net_buf_cp_short (net_buf, hh);
  net_buf_cp_short (net_buf, mm);
  net_buf_cp_short (net_buf, ss);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_TIME;
    }
}

static void
add_res_data_date (T_NET_BUF * net_buf, short yr, short mon, short day, unsigned char ext_type, int *net_size)
{
  if (ext_type)
    {
      net_buf_cp_int (net_buf, NET_BUF_TYPE_SIZE (net_buf) + NET_SIZE_DATE, NULL);
      net_buf_cp_cas_type_and_charset (net_buf, ext_type, CAS_SCHEMA_DEFAULT_CHARSET);
    }
  else
    {
      net_buf_cp_int (net_buf, NET_SIZE_DATE, NULL);
    }

  net_buf_cp_short (net_buf, yr);
  net_buf_cp_short (net_buf, mon);
  net_buf_cp_short (net_buf, day);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + (ext_type ? NET_BUF_TYPE_SIZE (net_buf) : 0) + NET_SIZE_DATE;
    }
}

static unsigned char
set_extended_cas_type (T_CCI_U_TYPE u_set_type, DB_TYPE db_type)
{
  /* todo: T_CCI_U_TYPE duplicates db types. */
  unsigned char u_set_type_lsb, u_set_type_msb;

#if 0
  if (TP_IS_SET_TYPE (db_type))
    {
      unsigned char cas_ext_type;

      u_set_type_lsb = u_set_type & 0x1f;
      u_set_type_msb = (u_set_type & 0x20) << 2;

      u_set_type = (T_CCI_U_TYPE) (u_set_type_lsb | u_set_type_msb);

      cas_ext_type = CAS_TYPE_COLLECTION (db_type, u_set_type);
      return cas_ext_type;
    }
#endif

  u_set_type = (T_CCI_U_TYPE) ux_db_type_to_cas_type (db_type);

  u_set_type_lsb = u_set_type & 0x1f;
  u_set_type_msb = (u_set_type & 0x20) << 2;

  u_set_type = (T_CCI_U_TYPE) (u_set_type_lsb | u_set_type_msb);

  return u_set_type;
}

static short
encode_ext_type_to_short (unsigned char cas_type)
{
  short ret_type;
  unsigned char msb_byte, lsb_byte;

  msb_byte = cas_type & CCI_CODE_COLLECTION;
  msb_byte |= CAS_TYPE_FIRST_BYTE_PROTOCOL_MASK;

  lsb_byte = CCI_GET_COLLECTION_DOMAIN (cas_type);

  ret_type = ((short) msb_byte << 8) | ((short) lsb_byte);

  return ret_type;
}

static int
get_num_markers (char *stmt, char **return_stmt)
{
  char *p;
  int num_markers = 0;

  for (p = stmt; *p; p++)
    {
      if (*p == '?')
	{
	  num_markers++;
	}
      else if (*p == '-' && *(p + 1) == '-')
	{
	  p = consume_tokens (p + 2, SQL_STYLE_COMMENT);
	}
      else if (*p == '/' && *(p + 1) == '*')
	{
	  p = consume_tokens (p + 2, C_STYLE_COMMENT);
	}
      else if (*p == '/' && *(p + 1) == '/')
	{
	  p = consume_tokens (p + 2, CPP_STYLE_COMMENT);
	}
      else if (*p == '\'')
	{
	  p = consume_tokens (p + 1, SINGLE_QUOTED_STRING);
	}
      else if (cas_default_ansi_quotes == false && *p == '\"')
	{
	  p = consume_tokens (p + 1, DOUBLE_QUOTED_STRING);
	}

      if (*p == '\0')
	{
	  break;
	}
    }

  if (num_markers > 99)
    {
      *return_stmt = NULL;
      return num_markers;
    }

  *return_stmt = (char *) malloc (strlen (stmt) + (num_markers * 3) + 1);
  if (*return_stmt != NULL)
    {
      if (num_markers == 0)
	{
	  strcpy (*return_stmt, stmt);
	}
      else
	{
	  char *dest = *return_stmt;
	  char *src = stmt;
	  char var[5];

	  for (int i = 1; *src != '\0'; dest++, src++)
	    {
	      if (*src == '?')
		{
		  sprintf (var, "$v%02d", i);
		  memcpy (dest, var, strlen (var));
		  dest = dest + strlen (var) - 1;
		}
	      else
		{
		  *dest = *src;
		}
	    }
	  *dest = '\0';
	}
    }

  return num_markers;
}

static char *
consume_tokens (char *stmt, STATEMENT_STATUS stmt_status)
{
  char *p = stmt;

  if (stmt_status == SQL_STYLE_COMMENT || stmt_status == CPP_STYLE_COMMENT)
    {
      for (; *p; p++)
	{
	  if (*p == '\n')
	    {
	      break;
	    }
	}
    }
  else if (stmt_status == C_STYLE_COMMENT)
    {
      for (; *p; p++)
	{
	  if (*p == '*' && *(p + 1) == '/')
	    {
	      p++;
	      break;
	    }
	}
    }
  else if (stmt_status == SINGLE_QUOTED_STRING)
    {
      for (; *p; p++)
	{
	  if (*p == '\'' && *(p + 1) == '\'')
	    {
	      p++;
	    }
	  else if (cas_default_no_backslash_escapes == false && *p == '\\')
	    {
	      p++;
	    }
	  else if (*p == '\'')
	    {
	      break;
	    }
	}
    }
  else if (stmt_status == DOUBLE_QUOTED_STRING)
    {
      for (; *p; p++)
	{
	  if (*p == '\"' && *(p + 1) == '\"')
	    {
	      p++;
	    }
	  else if (cas_default_no_backslash_escapes == false && *p == '\\')
	    {
	      p++;
	    }
	  else if (*p == '\"')
	    {
	      break;
	    }
	}
    }

  return p;
}

static char
get_stmt_type (char *stmt)
{
  if (strncasecmp (stmt, "create", 6) == 0)
    {
      return S62_STMT_CREATE;
    }
  else if (strncasecmp (stmt, "match", 6) == 0)
    {
      return S62_STMT_MATCH;
    }
  else if (strncasecmp (stmt, "call", 4) == 0)
    {
      return S62_STMT_CALL;
    }
  else
    {
      return S62_MAX_STMT_TYPE;
    }
}

static int
prepare_column_list_info_set (char prepare_flag, T_QUERY_RESULT * q_result, T_NET_BUF * net_buf)
{
  char stmt_type = q_result->stmt_type;
  S62_PROPERTY *property, *ptr;
  char updatable_flag = FALSE;	// prepare_flag & CCI_PREPARE_UPDATABLE
  int num_cols;
  int num_col_offset;
  unsigned char type;
  char *dot;
  int i = 0;

  q_result->col_updatable = FALSE;
  q_result->include_oid = FALSE;
  if (q_result->null_type_column != NULL)
    {
      FREE_MEM (q_result->null_type_column);
    }

  net_buf_cp_byte (net_buf, updatable_flag);

  num_cols = s62_get_property_from_statement (q_result->stmt_id, &property);
  net_buf_cp_int (net_buf, num_cols, NULL);

  for (ptr = property; ptr != NULL; ptr = ptr->next, i++)
    {
      // 1. data type
      type = set_extended_cas_type (CCI_U_TYPE_UNKNOWN, s62type_to_dbtype (ptr->property_type));
      net_buf_cp_cas_type_and_charset (net_buf, type, CAS_SCHEMA_DEFAULT_CHARSET);
      // 2. scale
      net_buf_cp_short (net_buf, ptr->scale);
      // 3. precision
      net_buf_cp_int (net_buf, ptr->precision, NULL);
      // 4. column name & 5. attr name
      if (strlen (ptr->property_name) <= 0)
	{
	  net_buf_cp_int (net_buf, (int) strlen ("_unknown") + 1, NULL);
	  net_buf_cp_str (net_buf, "_unknown", strlen ("_unknown") + 1);
	  net_buf_cp_int (net_buf, (int) strlen ("_unknown") + 1, NULL);
	  net_buf_cp_str (net_buf, "_unknown", strlen ("_unknown") + 1);
	}
      else
	{
	  for (dot = ptr->property_name; *dot != '\0'; dot++)
	    {
	      if (*dot == '.')
		break;
	    }

	  if (*dot == '.')
	    {
	      dot = dot + 1;
	      net_buf_cp_int (net_buf, (int) strlen (dot) + 1, NULL);
	      net_buf_cp_str (net_buf, dot, (int) strlen (dot) + 1);
	      net_buf_cp_int (net_buf, (int) strlen (dot) + 1, NULL);
	      net_buf_cp_str (net_buf, dot, (int) strlen (dot) + 1);
	    }
	  else
	    {
	      net_buf_cp_int (net_buf, (int) strlen (ptr->property_name) + 1, NULL);
	      net_buf_cp_str (net_buf, ptr->property_name, (int) strlen (ptr->property_name) + 1);
	      net_buf_cp_int (net_buf, (int) strlen (ptr->property_name) + 1, NULL);
	      net_buf_cp_str (net_buf, ptr->property_name, (int) strlen (ptr->property_name) + 1);
	    }
	}
      // 6. class name
      if (ptr->label_type == S62_OTHER || strlen (ptr->label_name) <= 0)
	{
	  net_buf_cp_int (net_buf, (int) strlen ("_others"), NULL);
	  net_buf_cp_str (net_buf, "_others", (int) strlen ("_others"));
	}
      else
	{
	  net_buf_cp_int (net_buf, (int) strlen (ptr->label_name) + 1, NULL);
	  net_buf_cp_str (net_buf, ptr->label_name, (int) strlen (ptr->label_name) + 1);
	}
      // 7. is not null
      net_buf_cp_byte (net_buf, 0);
      // 8. default value
      net_buf_cp_int (net_buf, 1, NULL);
      net_buf_cp_byte (net_buf, '\0');
      // 9. auto_increment
      net_buf_cp_byte (net_buf, 0);
      // 10.unique key
      net_buf_cp_byte (net_buf, 0);
      // 11.primary key
      net_buf_cp_byte (net_buf, 0);
      // 12.reverse index
      net_buf_cp_byte (net_buf, 0);
      // 13.reverse unique
      net_buf_cp_byte (net_buf, 0);
      // 14.foreign key
      net_buf_cp_byte (net_buf, 0);
      // 15.shared
      net_buf_cp_byte (net_buf, 0);
    }

  if (num_cols == i)
    {
      return 0;
    }
  else
    {
      return -1;
    }
}

static int
bind_from_netval (S62_STATEMENT * stmt_id, int idx, void *net_type, void *net_value, T_NET_BUF * net_buf,
		  char desired_type)
{
  char type;
  int err_code = 0;
  int data_size;
  DB_VALUE db_val;
  char coercion_flag = TRUE;

  net_arg_get_char (type, net_type);

  if (type == CCI_U_TYPE_STRING || type == CCI_U_TYPE_CHAR || type == CCI_U_TYPE_ENUM)
    {
      if (desired_type == DB_TYPE_NUMERIC)
	{
	  type = CCI_U_TYPE_NUMERIC;
	}
      else if (desired_type == DB_TYPE_NCHAR || desired_type == DB_TYPE_VARNCHAR)
	{
	  type = CCI_U_TYPE_NCHAR;
	}
    }

  if (type == CCI_U_TYPE_DATETIME)
    {
      if (desired_type == DB_TYPE_TIMESTAMP)
	{
	  type = CCI_U_TYPE_TIMESTAMP;
	}
      else if (desired_type == DB_TYPE_DATE)
	{
	  type = CCI_U_TYPE_DATE;
	}
      else if (desired_type == DB_TYPE_TIME)
	{
	  type = CCI_U_TYPE_TIME;
	}
    }
  else if (type == CCI_U_TYPE_DATETIMETZ)
    {
      if (desired_type == DB_TYPE_TIMESTAMPTZ || desired_type == DB_TYPE_TIMESTAMPLTZ)
	{
	  type = CCI_U_TYPE_TIMESTAMPTZ;
	}
    }

  net_arg_get_size (&data_size, net_value);
  if (data_size <= 0)
    {
      type = CCI_U_TYPE_NULL;
      data_size = 0;
    }

  switch (type)
    {
    case CCI_U_TYPE_NULL:
      coercion_flag = FALSE;
      break;
    case CCI_U_TYPE_CHAR:
    case CCI_U_TYPE_STRING:
    case CCI_U_TYPE_ENUM:
    case CCI_U_TYPE_NCHAR:
    case CCI_U_TYPE_VARNCHAR:
      {
	char *value, *invalid_pos = NULL;
	int val_size;

	net_arg_get_str (&value, &val_size, net_value);

	val_size--;

//      s62_bind_string (stmt_id, idx, value);
      }
      break;
    case CCI_U_TYPE_NUMERIC:
      {
	char *value, *p;
	int val_size;
	int precision, scale;
	char tmp[BUFSIZ];

	net_arg_get_str (&value, &val_size, net_value);
	if (value != NULL)
	  {
	    strcpy (tmp, value);
	  }
	tmp[val_size] = '\0';
	ut_trim (tmp);
	precision = strlen (tmp);
	p = strchr (tmp, '.');
	if (p == NULL)
	  {
	    scale = 0;
	  }
	else
	  {
	    scale = strlen (p + 1);
	    precision--;
	  }
	if (tmp[0] == '-')
	  {
	    precision--;
	  }
      }
      coercion_flag = FALSE;
      break;
    case CCI_U_TYPE_BIGINT:
      {
	int64_t bi_val;

	net_arg_get_bigint (&bi_val, net_value);
      }
      break;
    case CCI_U_TYPE_INT:
      {
	int i_val;

	net_arg_get_int (&i_val, net_value);

//      s62_bind_int (stmt_id, idx, i_val);
      }
      break;
    case CCI_U_TYPE_SHORT:
      {
	short s_val;

	net_arg_get_short (&s_val, net_value);

//      s62_bind_short (stmt_id, idx, s_val);
      }
      break;
    case CCI_U_TYPE_FLOAT:
      {
	float f_val;
	net_arg_get_float (&f_val, net_value);
      }
      break;
    case CCI_U_TYPE_DOUBLE:
      {
	double d_val;
	net_arg_get_double (&d_val, net_value);
      }
      break;
    case CCI_U_TYPE_DATE:
      {
	short month, day, year;
	net_arg_get_date (&year, &month, &day, net_value);
      }
      break;
    case CCI_U_TYPE_TIME:
      {
	short hh, mm, ss;
	net_arg_get_time (&hh, &mm, &ss, net_value);
      }
      break;
    case CCI_U_TYPE_TIMESTAMP:
      {
	short yr, mon, day, hh, mm, ss;
	net_arg_get_timestamp (&yr, &mon, &day, &hh, &mm, &ss, net_value);
      }
      break;
    case CCI_U_TYPE_DATETIME:
      {
	short yr, mon, day, hh, mm, ss, ms;

	net_arg_get_datetime (&yr, &mon, &day, &hh, &mm, &ss, &ms, net_value);
      }
      break;
    case CCI_U_TYPE_MONETARY:
    case CCI_U_TYPE_BIT:
    case CCI_U_TYPE_VARBIT:
    case CCI_U_TYPE_DATETIMELTZ:
    case CCI_U_TYPE_DATETIMETZ:
    case CCI_U_TYPE_TIMESTAMPTZ:
    case CCI_U_TYPE_TIMESTAMPLTZ:
    case CCI_U_TYPE_SET:
    case CCI_U_TYPE_MULTISET:
    case CCI_U_TYPE_SEQUENCE:
    case CCI_U_TYPE_OBJECT:
    case CCI_U_TYPE_CLOB:
    case CCI_U_TYPE_BLOB:
    case CCI_U_TYPE_JSON:
    case CCI_U_TYPE_USHORT:
    case CCI_U_TYPE_UINT:
    case CCI_U_TYPE_UBIGINT:
    default:
      return ERROR_INFO_SET (CAS_ER_UNKNOWN_U_TYPE, CAS_ERROR_INDICATOR);
    }

  if (err_code < 0)
    {
      return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
    }

  return data_size;
}

static int
value_to_netbuf (S62_RESULTSET * resultset, T_NET_BUF * net_buf, int idx, int max_col_size, char column_type_flag)
{
  int data_size = 0;
  unsigned char ext_col_type = 0;
  bool client_support_tz = false;

#if 0
  if (column_type_flag == DB_TYPE_NULL)
    {
      net_buf_cp_int (net_buf, -1, NULL);
      return NET_SIZE_INT;
    }
#endif

#if 0
  ext_col_type = set_extended_cas_type (CCI_U_TYPE_NULL, (DB_TYPE) column_type_flag);
#else
  ext_col_type = 0;
#endif
  switch (column_type_flag)
    {
    case S62_TYPE_VARCHAR:
      {
	s62_string str;
	int bytes_size = 0;

	str = s62_get_varchar (resultset, idx);
	if (str.data == NULL || str.size == 0)
	  {
	    net_buf_cp_int (net_buf, -1, NULL);
	    data_size += NET_SIZE_INT;
	    break;
	  }
	bytes_size = str.size;
	if (max_col_size > 0)
	  {
	    bytes_size = MIN (bytes_size, max_col_size);
	  }

//      printf ("str.data : %s\n", str.data);
	add_res_data_string (net_buf, str.data, bytes_size, ext_col_type, CAS_SCHEMA_DEFAULT_CHARSET, &data_size);
      }
      break;
    case S62_TYPE_SMALLINT:
      {
	int16_t smallint;
	smallint = s62_get_int16 (resultset, idx);
	add_res_data_short (net_buf, smallint, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_USMALLINT:
      {
	uint16_t smallint;
	smallint = s62_get_uint16 (resultset, idx);
	add_res_data_short (net_buf, smallint, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_INTEGER:
      {
	int32_t int_val;
	int_val = s62_get_int32 (resultset, idx);
	add_res_data_int (net_buf, int_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_UINTEGER:
      {
	uint32_t int_val;
	int_val = s62_get_uint32 (resultset, idx);
	add_res_data_int (net_buf, int_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_BIGINT:
      {
	int64_t bigint_val;
	bigint_val = s62_get_int64 (resultset, idx);
	add_res_data_bigint (net_buf, bigint_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_UBIGINT:
      {
	uint64_t bigint_val;
	bigint_val = s62_get_uint64 (resultset, idx);
	add_res_data_bigint (net_buf, bigint_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_HUGEINT:
      {
    s62_hugeint hugeint_val;
    s62_string str;

    hugeint_val = s62_get_hugeint(resultset, idx);
    str = hugeint_to_string(&hugeint_val);

    add_res_data_string (net_buf, str.data, str.size, ext_col_type, CAS_SCHEMA_DEFAULT_CHARSET, &data_size);
      }
      break;
    case S62_TYPE_ID:
      {
	uint64_t bigint_val;
	bigint_val = s62_get_id (resultset, idx);
	add_res_data_bigint (net_buf, bigint_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_DOUBLE:
      {
	double d_val;
	d_val = s62_get_double (resultset, idx);
	add_res_data_double (net_buf, d_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_FLOAT:
      {
	float f_val;
	f_val = s62_get_float (resultset, idx);
	add_res_data_float (net_buf, f_val, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_DECIMAL:
      {
	s62_decimal f_decimal;
	s62_string str;

	f_decimal = s62_get_decimal (resultset, idx);
	str = s62_decimal_to_string (f_decimal);
	add_res_data_string (net_buf, str.data, str.size, ext_col_type, CAS_SCHEMA_DEFAULT_CHARSET, &data_size);
      }
      break;
    case S62_TYPE_DATE:
      {
  s62_date date_val;
  int yr, mon, day;
  date_val = s62_get_date (resultset, idx);
  days_to_date(date_val, &yr, &mon, &day);
  add_res_data_date (net_buf, (short) yr, (short) mon, (short) day, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_TIME:
      {
  s62_time time_val;
  int hour, minute, second;
  time_val = s62_get_time(resultset, idx);
  int64_t total_seconds = time_val.micros / 1000000;
  hour = total_seconds / 3600;
  minute = (total_seconds % 3600) / 60;
  second = total_seconds % 60;
  add_res_data_time (net_buf, (short) hour, (short) minute, (short) second, ext_col_type, &data_size);
      }
      break;
    case S62_TYPE_TIMESTAMP:
      {
  s62_timestamp timestamp;
  int yr, mon, day, hh, mm, ss;
  timestamp = s62_get_timestamp(resultset, idx);
  time_to_timestamp(timestamp.micros, &yr, &mon, &day, &hh, &mm, &ss);
  add_res_data_timestamp (net_buf, (short) yr, (short) mon, (short) day, (short) hh, (short) mm, (short) ss,
  ext_col_type, &data_size);
      }
      break;
    default:
      net_buf_cp_int (net_buf, -1, NULL);	/* null */
      data_size = 4;
      break;
    }

  return data_size;
}

static int
execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf, T_BROKER_VERSION client_version, char exec_flag)
{
  return 0;
}

static int
sch_class_info (T_NET_BUF * net_buf, char *class_name, char pattern_flag, char v_class_flag, void **result)
{
  S62_METADATA *meta_res;
  int num_results = 0;

  cas_log_write (0, true, "--sch_class_info");
  *result = (void *) NULL;

  num_results = s62_get_metadata_from_catalog (class_name, pattern_flag, v_class_flag, &meta_res);
  if (num_results < 0)
    {
      num_results = 0;
    }
  else
    {
      *result = meta_res;
    }

  net_buf_cp_int (net_buf, num_results, NULL);
  schema_table_meta (net_buf);

  return 0;
}

static int
sch_attr_info (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pattern_flag, char class_attr_flag,
	       void **result)
{
  S62_PROPERTY *property_res;
  s62_metadata_type type;
  int num_results = 0;

  cas_log_write (0, true, "--sch_attr_info [%s] [%s]", class_name, attr_name);
  *result = (void *) NULL;

  if (strcmp (attr_name, "node") == 0)
    {
      type = S62_NODE;
    }
  else if (strcmp (attr_name, "edge") == 0)
    {
      type = S62_EDGE;
    }
  else
    {
      type = S62_OTHER;
    }

  cas_log_write (0, true, "--s62_get_property_from_catalog [%s] [%d]  -- before call", class_name, type);
  num_results = s62_get_property_from_catalog (class_name, type, &property_res);
  cas_log_write (0, true, "--s62_get_property_from_catalog -- after call");
  if (num_results < 0)
    {
      num_results = 0;
    }
  else
    {
      *result = property_res;
    }

  cas_log_write (0, true, "--net_buf_cp_int");
  net_buf_cp_int (net_buf, num_results, NULL);
  cas_log_write (0, true, "--schema_attr_meta");
  schema_attr_meta (net_buf);

  return 0;
}

int
ux_auto_commit (T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  cas_log_write (0, true, "--ux_auto_commit");
  return 0;
}

static bool
has_stmt_result_set (char stmt_type)
{
  switch (stmt_type)
    {
    case S62_STMT_MATCH:
      return true;

    default:
      break;
    }

  return false;
}

static bool
check_auto_commit_after_getting_result (T_SRV_HANDLE * srv_handle)
{
  // To close an updatable cursor is dangerous since it lose locks and updating cursor is allowed before closing it.

  if (srv_handle->auto_commit_mode == TRUE && srv_handle->cur_result_index == srv_handle->num_q_result
      && srv_handle->forward_only_cursor == TRUE && srv_handle->is_updatable == FALSE)
    {
      return true;
    }

  return false;
}

void
cas_set_db_connect_status (int status)
{
}

int
cas_get_db_connect_status (void)
{
  return 0;
}

char *
cas_log_error_handler_asprint (char *buf, size_t bufsz, bool clear)
{
  char *buf_p;
  unsigned int from, to;

  if (buf == NULL || bufsz <= 0)
    {
      return NULL;
    }

  if (cas_EHCTX == NULL || cas_EHCTX->from == 0)
    {
      buf[0] = '\0';
      return buf;
    }

  from = cas_EHCTX->from;
  to = cas_EHCTX->to;

  if (clear)
    {
      cas_EHCTX->from = 0;
      cas_EHCTX->to = 0;
    }

  /* ", EID = <int> ~ <int>" : 32 bytes suffice */
  if (bufsz < 32)
    {
      buf_p = (char *) malloc (32);

      if (buf_p == NULL)
	{
	  return NULL;
	}
    }
  else
    {
      buf_p = buf;
    }

  /* actual print */
  if (to != 0)
    {
      snprintf (buf_p, 32, ", EID = %u ~ %u", from, to);
    }
  else
    {
      snprintf (buf_p, 32, ", EID = %u", from);
    }

  return buf_p;
}

int
get_tuple_count (T_SRV_HANDLE * srv_handle)
{
  return srv_handle->q_result->tuple_count;
}

/*
 * get_backslash_escape_string() - This function returns proper backslash escape
 * string according to the value of 'no_backslash_escapes' configuration.
 */
static char *
get_backslash_escape_string (void)
{
  return (char *) "\\\\";
}

static void
update_query_execution_count (T_APPL_SERVER_INFO * as_info_p, char stmt_type)
{
  assert (as_info_p != NULL);

  as_info_p->num_queries_processed %= MAX_DIAG_DATA_VALUE;
  as_info_p->num_queries_processed++;

  switch (stmt_type)
    {
    case S62_STMT_MATCH:
      as_info_p->num_select_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_select_queries++;
      break;
    case S62_STMT_INSERT:
      as_info_p->num_insert_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_insert_queries++;
      break;
    case S62_STMT_UPDATE:
      as_info_p->num_update_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_update_queries++;
      break;
    case S62_STMT_DELETE:
      as_info_p->num_delete_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_delete_queries++;
      break;
    default:
      break;
    }
}

static bool
need_reconnect_on_rctime (void)
{
  return false;
}

static void
report_abnormal_host_status (int err_code)
{
}

//
// do_commit_after_execute () - commit transaction immediately after executing query or queries.
//
// return             : true to commit, false otherwise
// server_handle (in) : server handle
//
static bool
do_commit_after_execute (const t_srv_handle & server_handle)
{
  return false;
}

static int
recompile_statement (T_SRV_HANDLE * srv_handle)
{
  return 0;
}

static s62_string
hugeint_to_string(s62_hugeint *hugeint)
{
  char str[40];

  __int128 combinedValue = ((__int128)hugeint->upper << 64) | hugeint->lower;
  snprintf(str, sizeof(str), "%" PRIu64 "%" PRIu64, (uint64_t)(combinedValue >> 64), (uint64_t)combinedValue);

  s62_string result;
  result.size = strlen(str);
  result.data = (char*)malloc(result.size + 1);
  strcpy(result.data, str);

  return result;

}

static void
time_to_timestamp(int64_t time, int *year, int *month, int *day, int *hh, int *mm, int *ss)
{
  //1970-01-01
  struct tm base_date = {0};
  base_date.tm_year = 70;
  base_date.tm_mon = 0;
  base_date.tm_mday = 1;

  time_t base_time = mktime(&base_date);
  time_t real_time = base_time + time;
  struct tm *real_tm = localtime(&real_time);
  *year = real_tm->tm_year + 1900;
  *month = real_tm->tm_mon + 1;
  *day = real_tm->tm_mday;
  *hh = real_tm->tm_hour;
  *mm = real_tm->tm_min;
  *ss = real_tm->tm_sec;
}

static void
days_to_date(s62_date date, int *year, int *month, int *day)
{
  int64_t time = (date.days * 24 * 60 * 60);
  int hh, mm, ss;
  time_to_timestamp(time, year, month, day, &hh, &mm, &ss);
}
