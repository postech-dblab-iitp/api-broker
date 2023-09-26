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

static void prepare_column_info_set (T_NET_BUF * net_buf, char ut, short scale, int prec, char charset,
				     const char *col_name, const char *default_value, char auto_increment,
				     char unique_key, char primary_key, char reverse_index, char reverse_unique,
				     char foreign_key, char shared, const char *attr_name, const char *class_name,
				     char nullable, T_BROKER_VERSION client_version);
static void set_column_info (T_NET_BUF * net_buf, char ut, short scale, int prec, char charset, const char *col_name,
			     const char *attr_name, const char *class_name, char is_non_null,
			     T_BROKER_VERSION client_version);

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
static int get_num_markers (char *stmt);
static char *consume_tokens (char *stmt, STATEMENT_STATUS stmt_status);
static char get_stmt_type (char *stmt);
static int execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf, T_BROKER_VERSION client_version,
			     char exec_flag);
static int sch_class_info (T_NET_BUF * net_buf, char *class_name, char pattern_flag, char flag, T_SRV_HANDLE *,
			   T_BROKER_VERSION client_version);
static int sch_attr_info (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pattern_flag, char flag,
			  T_SRV_HANDLE *);
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

static short encode_ext_type_to_short (T_BROKER_VERSION client_version, unsigned char cas_type);
static int ux_get_generated_keys_server_insert (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf);
static int ux_get_generated_keys_client_insert (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf);

static bool do_commit_after_execute (const t_srv_handle & server_handle);
static int recompile_statement (T_SRV_HANDLE * srv_handle);

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
};

#if defined (FOR_API_CAS)
static T_FETCH_FUNC fetch_func[] = {
  fetch_result,			/* query */
  fetch_not_supported,		/* SCH_CLASS */
  fetch_not_supported,		/* SCH_VCLASS */
  fetch_not_supported,		/* SCH_QUERY_SPEC */
  fetch_not_supported,		/* SCH_ATTRIBUTE */
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
 return 0;
}

int
ux_database_connect (char *db_name, char *db_user, char *db_passwd, char **db_err_msg)
{
 return 0;
}

int
ux_is_database_connected (void)
{
 return 0;
}

void
ux_database_shutdown ()
{
}

int
ux_prepare (char *sql_stmt, int flag, char auto_commit_mode, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
	    unsigned int query_seq_num)
{
 return 0;
}

int
ux_end_tran (int tran_type, bool reset_con_status)
{
 return 0;
}

int
ux_get_row_count (T_NET_BUF * net_buf)
{
 return 0;
}

int
ux_get_last_insert_id (T_NET_BUF * net_buf)
{
 return 0;
}

int
ux_execute (T_SRV_HANDLE * srv_handle, char flag, int max_col_size, int max_row, int argc, void **argv,
	    T_NET_BUF * net_buf, T_REQ_INFO * req_info, CACHE_TIME * clt_cache_time, int *clt_cache_reusable)
{
 return 0;
}

int
ux_execute_all (T_SRV_HANDLE * srv_handle, char flag, int max_col_size, int max_row, int argc, void **argv,
		T_NET_BUF * net_buf, T_REQ_INFO * req_info, CACHE_TIME * clt_cache_time, int *clt_cache_reusable)
{
 return 0;
}

int
ux_next_result (T_SRV_HANDLE * srv_handle, char flag, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
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
}

int
ux_fetch (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_index,
	  T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

int
ux_cursor (int srv_h_id, int offset, int origin, T_NET_BUF * net_buf)
{
 return 0;
}

void
ux_cursor_close (T_SRV_HANDLE * srv_handle)
{
}

int
ux_get_db_version (T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

int
ux_get_class_num_objs (char *class_name, int flag, T_NET_BUF * net_buf)
{
 return 0;
}

int
make_bind_value (int num_bind, int argc, void **argv, void ** ret_val, T_NET_BUF * net_buf, char desired_type)
{
 return 0;
}

int
ux_get_attr_type_str (char *class_name, char *attr_name, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

int
ux_get_query_info (int srv_h_id, char info_type, T_NET_BUF * net_buf)
{
 return 0;
}

void
ux_free_result (void *res)
{
}

int
ux_schema_info (int schema_type, char *arg1, char *arg2, char flag, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
		unsigned int query_seq_num)
{
 return 0;
}

static int
fetch_result (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
	      T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

static int
fetch_class (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
	     T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

static int
fetch_attribute (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
		 T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}


static int
fetch_not_supported (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count, char fetch_flag, int result_set_idx,
		     T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  return ERROR_INFO_SET (CAS_ER_NOT_IMPLEMENTED, CAS_ERROR_INDICATOR);
}


static int
get_num_markers (char *stmt)
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
  if (strncasecmp (stmt, "insert", 6) == 0)
    {
      return CUBRID_STMT_INSERT;
    }
  else if (strncasecmp (stmt, "update", 6) == 0)
    {
      return CUBRID_STMT_UPDATE;
    }
  else if (strncasecmp (stmt, "delete", 6) == 0)
    {
      return CUBRID_STMT_DELETE;
    }
  else if (strncasecmp (stmt, "call", 4) == 0)
    {
      return CUBRID_STMT_CALL;
    }
  else if (strncasecmp (stmt, "evaluate", 8) == 0)
    {
      return CUBRID_STMT_EVALUATE;
    }
  else if (strncasecmp (stmt, "select", 6) == 0)
    {
      return CUBRID_STMT_SELECT;
    }
  else if (strncasecmp (stmt, "merge", 5) == 0)
    {
      return CUBRID_STMT_MERGE;
    }
  else
    {
      return CUBRID_MAX_STMT_TYPE;
    }
}

static int
execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf, T_BROKER_VERSION client_version, char exec_flag)
{
 return 0;
}

static int
sch_class_info (T_NET_BUF * net_buf, char *class_name, char pattern_flag, char v_class_flag, T_SRV_HANDLE * srv_handle,
		T_BROKER_VERSION client_version)
{
 return 0;
}

static int
sch_attr_info (T_NET_BUF * net_buf, char *class_name, char *attr_name, char pattern_flag, char class_attr_flag,
	       T_SRV_HANDLE * srv_handle)
{
 return 0;
}

int
ux_auto_commit (T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
 return 0;
}

static bool
has_stmt_result_set (char stmt_type)
{
  switch (stmt_type)
    {
    case CUBRID_STMT_SELECT:
    case CUBRID_STMT_CALL:
    case CUBRID_STMT_GET_STATS:
    case CUBRID_STMT_EVALUATE:
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
 return 0;
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
    case CUBRID_STMT_SELECT:
      as_info_p->num_select_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_select_queries++;
      break;
    case CUBRID_STMT_INSERT:
      as_info_p->num_insert_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_insert_queries++;
      break;
    case CUBRID_STMT_UPDATE:
      as_info_p->num_update_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_update_queries++;
      break;
    case CUBRID_STMT_DELETE:
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
