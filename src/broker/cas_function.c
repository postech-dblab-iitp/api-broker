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
 * cas_function.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#include <sys/timeb.h>
#include <process.h>
#else
#include <unistd.h>
#include <sys/time.h>
#endif
#include <assert.h>

#include "error_manager.h"

#include "cas_common.h"
#include "cas.h"
#include "cas_function.h"
#include "cas_network.h"
#include "cas_net_buf.h"
#include "cas_log.h"
#include "cas_handle.h"
#include "cas_util.h"
#include "cas_execute.h"

#include "broker_filename.h"
#include "cas_sql_log2.h"
#include "intl_support.h"

static FN_RETURN fn_prepare_internal (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
				      int *ret_srv_h_id);
static FN_RETURN fn_execute_internal (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
				      int *prepared_srv_h_id);
static const char *get_schema_type_str (int schema_type);
static const char *get_tran_type_str (int tran_type);
static void bind_value_print (char type, void *net_value, bool slow_log);
static char *get_error_log_eids (int err);

static void bind_value_log (struct timeval *log_time, int start, int argc, void **argv, int param_size,
			    char *param_mode, unsigned int query_seq_num, bool slow_log);
static void update_error_query_count (T_APPL_SERVER_INFO * as_info_p, const T_ERROR_INFO * err_info_p);

static const char *tran_type_str[] = { "COMMIT", "ROLLBACK" };

static const char *schema_type_str[] = {
  "CLASS",
  "VCLASS",
  "QUERY_SPEC",
  "ATTRIBUTE",
  "CLASS_ATTRIBUTE",
  "METHOD",
  "CLASS_METHOD",
  "METHOD_FILE",
  "SUPERCLASS",
  "SUBCLASS",
  "CONSTRAINT",
  "TRIGGER",
  "CLASS_PRIVILEGE",
  "ATTR_PRIVILEGE",
  "DIRECT_SUPER_CLASS",
  "PRIMARY_KEY",
  "IMPORTED_KEYS",
  "EXPORTED_KEYS",
  "CROSS_REFERENCE"
};

static const char *type_str_tbl[] = {
  "NULL",			/* CCI_U_TYPE_NULL */
  "CHAR",			/* CCI_U_TYPE_CHAR */
  "VARCHAR",			/* CCI_U_TYPE_STRING */
  "NCHAR",			/* CCI_U_TYPE_NCHAR */
  "VARNCHAR",			/* CCI_U_TYPE_VARNCHAR */
  "BIT",			/* CCI_U_TYPE_BIT */
  "VARBIT",			/* CCI_U_TYPE_VARBIT */
  "NUMERIC",			/* CCI_U_TYPE_NUMERIC */
  "INT",			/* CCI_U_TYPE_INT */
  "SHORT",			/* CCI_U_TYPE_SHORT */
  "MONETARY",			/* CCI_U_TYPE_MONETARY */
  "FLOAT",			/* CCI_U_TYPE_FLOAT */
  "DOUBLE",			/* CCI_U_TYPE_DOUBLE */
  "DATE",			/* CCI_U_TYPE_DATE */
  "TIME",			/* CCI_U_TYPE_TIME */
  "TIMESTAMP",			/* CCI_U_TYPE_TIMESTAMP */
  "SET",			/* CCI_U_TYPE_SET */
  "MULTISET",			/* CCI_U_TYPE_MULTISET */
  "SEQUENCE",			/* CCI_U_TYPE_SEQUENCE */
  "OBJECT",			/* CCI_U_TYPE_OBJECT */
  "RESULTSET",			/* CCI_U_TYPE_RESULTSET */
  "BIGINT",			/* CCI_U_TYPE_BIGINT */
  "DATETIME",			/* CCI_U_TYPE_DATETIME */
  "BLOB",			/* CCI_U_TYPE_BLOB */
  "CLOB",			/* CCI_U_TYPE_CLOB */
  "ENUM",			/* CCI_U_TYPE_ENUM */
  "USHORT",			/* CCI_U_TYPE_USHORT */
  "UINT",			/* CCI_U_TYPE_UINT */
  "UBIGINT",			/* CCI_U_TYPE_UBIGINT */
  "TIMESTAMPTZ",		/* CCI_U_TYPE_TIMESTAMPTZ */
  "TIMESTAMPLTZ",		/* CCI_U_TYPE_TIMESTAMPLTZ */
  "DATETIMETZ",			/* CCI_U_TYPE_DATETIMETZ */
  "DATETIMELTZ",		/* CCI_U_TYPE_DATETIMELTZ */
  "TIMETZ",			/* CCI_U_TYPE_TIMETZ */
  "JSON",			/* CCI_U_TYPE_JSON */
};

FN_RETURN
fn_end_tran (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int tran_type;
  int err_code;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval end_tran_begin, end_tran_end;

  int timeout;


  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_char (tran_type, argv[0]);
  if (tran_type != CCI_TRAN_COMMIT && tran_type != CCI_TRAN_ROLLBACK)
    {
      ERROR_INFO_SET (CAS_ER_TRAN_TYPE, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_log_write (0, false, "end_tran %s", get_tran_type_str (tran_type));

  gettimeofday (&end_tran_begin, NULL);

  err_code = ux_end_tran ((char) tran_type, false);

  if ((tran_type == CCI_TRAN_ROLLBACK) && (req_info->client_version < CAS_MAKE_VER (8, 2, 0)))
    {
      /* For backward compatibility */
      cas_send_result_flag = FALSE;
    }

  gettimeofday (&end_tran_end, NULL);
  ut_timeval_diff (&end_tran_begin, &end_tran_end, &elapsed_sec, &elapsed_msec);

  cas_log_write (0, false, "end_tran %s%d time %d.%03d%s", err_code < 0 ? "error:" : "", err_info.err_number,
		 elapsed_sec, elapsed_msec, get_error_log_eids (err_info.err_number));

  if (err_code < 0)
    {
      NET_BUF_ERR_SET (net_buf);
      req_info->need_rollback = TRUE;
    }
  else
    {
      net_buf_cp_int (net_buf, 0, NULL);
      req_info->need_rollback = FALSE;
    }

  timeout =
    ut_check_timeout (&tran_start_time, &end_tran_end, shm_appl->long_transaction_time, &elapsed_sec, &elapsed_msec);
  if (timeout >= 0)
    {
      as_info->num_long_transactions %= MAX_DIAG_DATA_VALUE;
      as_info->num_long_transactions++;
    }
  if (err_code < 0 || errors_in_transaction > 0)
    {
      cas_log_end (SQL_LOG_MODE_ERROR, elapsed_sec, elapsed_msec);
      errors_in_transaction = 0;
    }
  else
    {
      if (timeout >= 0 || query_timeout >= 0)
	{
	  cas_log_end (SQL_LOG_MODE_TIMEOUT, elapsed_sec, elapsed_msec);
	}
      else
	{
	  cas_log_end (SQL_LOG_MODE_NONE, elapsed_sec, elapsed_msec);
	}
    }

  gettimeofday (&tran_start_time, NULL);
  gettimeofday (&query_start_time, NULL);
  tran_timeout = 0;
  query_timeout = 0;

  assert (as_info->con_status == CON_STATUS_IN_TRAN);
  as_info->con_status = CON_STATUS_OUT_TRAN;
  as_info->transaction_start_time = (time_t) 0;
  if (as_info->cas_log_reset)
    {
      cas_log_reset (broker_name);
    }
  if (as_info->cas_slow_log_reset)
    {
      cas_slow_log_reset (broker_name);
    }
  if (shm_appl->sql_log2 != as_info->cur_sql_log2)
    {
      sql_log2_end (false);
      as_info->cur_sql_log2 = shm_appl->sql_log2;
      sql_log2_init (broker_name, shm_as_index, as_info->cur_sql_log2, true);
    }

  if (!ux_is_database_connected ())
    {
      cas_log_debug (ARG_FILE_LINE, "fn_end_tran: !ux_is_database_connected()");
      return FN_CLOSE_CONN;
    }
  else if (restart_is_needed () || as_info->reset_flag == TRUE)
    {
      cas_log_debug (ARG_FILE_LINE, "fn_end_tran: restart_is_needed() || reset_flag");
      return FN_KEEP_SESS;
    }
  return FN_KEEP_CONN;
}

FN_RETURN
fn_end_session (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  /* ignore all request to close session from drivers */
  net_buf_cp_int (net_buf, NO_ERROR, NULL);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_row_count (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
#if !defined (FOR_API_CAS)
  ux_get_row_count (net_buf);
#endif
  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_last_insert_id (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
#if !defined (FOR_API_CAS)
  ux_get_last_insert_id (net_buf);
#endif
  return FN_KEEP_CONN;
}

FN_RETURN
fn_prepare (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  return (fn_prepare_internal (sock_fd, argc, argv, net_buf, req_info, NULL));
}


static FN_RETURN
fn_prepare_internal (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
		     int *ret_srv_h_id)
{
  char *sql_stmt;
  char flag;
  char auto_commit_mode;
  int sql_size;
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;
  int i;

  if (argc < 2)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_str (&sql_stmt, &sql_size, argv[0]);

  net_arg_get_char (flag, argv[1]);
  if (argc > 2)
    {
      net_arg_get_char (auto_commit_mode, argv[2]);
      if (cas_shard_flag == OFF)
	{
	  for (i = 3; i < argc; i++)
	    {
	      int deferred_close_handle;
	      net_arg_get_int (&deferred_close_handle, argv[i]);
	      cas_log_write (0, true, "close_req_handle srv_h_id %d", deferred_close_handle);
	      hm_srv_handle_free (deferred_close_handle);
	    }
	}
    }
  else
    {
      auto_commit_mode = FALSE;
    }

  gettimeofday (&query_start_time, NULL);
  query_timeout = 0;


  cas_log_write_nonl (query_seq_num_next_value (), false, "prepare %d ", flag);
  cas_log_write_query_string (sql_stmt, sql_size - 1);


  SQL_LOG2_COMPILE_BEGIN (as_info->cur_sql_log2, ((const char *) sql_stmt));

  /* append query string to as_info->log_msg */
  if (sql_stmt)
    {
      char *s, *t;
      size_t l;

      for (s = as_info->log_msg, l = 0; *s && l < SHM_LOG_MSG_SIZE - 1; s++, l++)
	{
	  /* empty body */
	}
      *s++ = ' ';
      l++;
      for (t = sql_stmt; *t && l < SHM_LOG_MSG_SIZE - 1; s++, t++, l++)
	{
	  *s = *t;
	}
      *s = '\0';
    }

  srv_h_id = ux_prepare (sql_stmt, flag, auto_commit_mode, net_buf, req_info, query_seq_num_current_value ());

  if (ret_srv_h_id != NULL)
    {
      /* this ret_srv_h_id used by following fn_execute_internal() function */
      *ret_srv_h_id = srv_h_id;
    }

  srv_handle = hm_find_srv_handle (srv_h_id);

  cas_log_write (query_seq_num_current_value (), false, "prepare srv_h_id %s%d%s%s", (srv_h_id < 0) ? "error:" : "",
		 (srv_h_id < 0) ? err_info.err_number : srv_h_id, (srv_handle != NULL
								   && srv_handle->use_plan_cache) ? " (PC)" : "",
		 get_error_log_eids (err_info.err_number));

  if (srv_h_id < 0)
    {
      update_error_query_count (as_info, &err_info);
    }


  return FN_KEEP_CONN;
}

FN_RETURN
fn_execute (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  FN_RETURN ret = fn_execute_internal (sock_fd, argc, argv, net_buf, req_info, NULL);

  return ret;
}

static FN_RETURN
fn_execute_internal (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info,
		     int *prepared_srv_h_id)
{
  int srv_h_id;
  char flag;
  int max_col_size;
  int bind_value_index;
  int max_row = 0;
  int ret_code;
  int param_mode_size = 0;
  char forward_only_cursor = 0;
  char auto_commit_mode = 0;
  char *param_mode = NULL;
  T_SRV_HANDLE *srv_handle;
  const char *exec_func_name;
  bool is_execute_call = false;
  int argc_mod_2;
  int (*ux_exec_func) (T_SRV_HANDLE *, char, int, int, int, void **, T_NET_BUF *, T_REQ_INFO *, CACHE_TIME *, int *);
  char fetch_flag = 0;
  CACHE_TIME clt_cache_time, *clt_cache_time_ptr;
  int client_cache_reusable = FALSE;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval exec_begin, exec_end;
  int app_query_timeout;
  bool client_supports_query_timeout = false;
  char *eid_string;
  int err_number_execute;
  int arg_idx = 0;
  char stmt_type = -1;

  char *plan = NULL;

  bind_value_index = 9;
  /*
   * query timeout is transferred from a driver only if protocol version 1
   * or above.
   */
  if (req_info->client_version >= CAS_PROTO_MAKE_VER (PROTOCOL_V1))
    {
      client_supports_query_timeout = true;
      bind_value_index++;
    }

  argc_mod_2 = bind_value_index % 2;

  if ((argc < bind_value_index) || (argc % 2 != argc_mod_2))
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  if (prepared_srv_h_id != NULL)
    {
      srv_h_id = *prepared_srv_h_id;
    }
  else
    {
      net_arg_get_int (&srv_h_id, argv[arg_idx++]);
    }
  srv_handle = hm_find_srv_handle (srv_h_id);

  if (srv_handle == NULL)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);

      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "execute_internal srv_h_id %d %s%d",
		     srv_h_id, "error:", err_info.err_number);

      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_char (flag, argv[arg_idx++]);
  net_arg_get_int (&max_col_size, argv[arg_idx++]);
  net_arg_get_int (&max_row, argv[arg_idx++]);
  net_arg_get_str (&param_mode, &param_mode_size, argv[arg_idx++]);
  if (prepared_srv_h_id != NULL)
    {
      if (srv_handle->stmt_type == S62_STMT_MATCH)
	{
	  fetch_flag = 1;
	}
      else
	{
	  fetch_flag = 0;
	}

      if (srv_handle->auto_commit_mode != 0)
	{
	  auto_commit_mode = true;
	  forward_only_cursor = true;
	}
      else
	{
	  auto_commit_mode = false;
	  forward_only_cursor = false;
	}
    }
  else
    {
      /* PROTOCOL_V2 is used only 9.0.0 */
      if (DOES_CLIENT_MATCH_THE_PROTOCOL (req_info->client_version, PROTOCOL_V2))
	{
	  if (srv_handle->stmt_type == S62_STMT_MATCH)
	    {
	      fetch_flag = 1;
	    }
	  else
	    {
	      fetch_flag = 0;
	    }

	  arg_idx++;		/* skip fetch_flag from driver */
	}
      else
	{
	  net_arg_get_char (fetch_flag, argv[arg_idx++]);
	}

      net_arg_get_char (auto_commit_mode, argv[arg_idx++]);
      net_arg_get_char (forward_only_cursor, argv[arg_idx++]);
    }

  clt_cache_time_ptr = &clt_cache_time;
  net_arg_get_cache_time (clt_cache_time_ptr, argv[arg_idx++]);

  if (client_supports_query_timeout == true)
    {
      net_arg_get_int (&app_query_timeout, argv[arg_idx++]);

      if (!DOES_CLIENT_UNDERSTAND_THE_PROTOCOL (req_info->client_version, PROTOCOL_V2))
	{
	  /* protocol version v1 driver send query timeout in second */
	  app_query_timeout *= 1000;
	}
    }
  else
    {
      app_query_timeout = 0;
    }


  if (shm_appl->max_string_length >= 0)
    {
      if (max_col_size <= 0 || max_col_size > shm_appl->max_string_length)
	max_col_size = shm_appl->max_string_length;
    }


  srv_handle->auto_commit_mode = auto_commit_mode;
  srv_handle->forward_only_cursor = forward_only_cursor;

  if (srv_handle->prepare_flag & CCI_PREPARE_CALL)	// || flag & CCI_EXEC_QUERY_ALL)
    {
      ERROR_INFO_SET (CAS_ER_INTERNAL, CAS_ERROR_INDICATOR);
      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "can't support execute mode");
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }
  else
    {
      exec_func_name = "execute";
      ux_exec_func = ux_execute;
      is_execute_call = false;
    }

  if (srv_handle->is_pooled)
    {
      gettimeofday (&query_start_time, NULL);
      query_timeout = 0;
    }


  cas_log_write_nonl (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "%s srv_h_id %d ", exec_func_name, srv_h_id);
  if (srv_handle->sql_stmt != NULL)
    {
      cas_log_write_query_string (srv_handle->sql_stmt, (int) strlen (srv_handle->sql_stmt));
    }
  cas_log_debug (ARG_FILE_LINE, "%s%s", auto_commit_mode ? "auto_commit_mode " : "",
		 forward_only_cursor ? "forward_only_cursor " : "");


  if (as_info->cur_sql_log_mode != SQL_LOG_MODE_NONE)
    {
      bind_value_log (NULL, bind_value_index, argc, argv, param_mode_size, param_mode,
		      SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false);
    }



  /* append query string to as_info->log_msg */
  if (srv_handle->sql_stmt)
    {
      char *s, *t;
      size_t l;

      for (s = as_info->log_msg, l = 0; *s && l < SHM_LOG_MSG_SIZE - 1; s++, l++)
	{
	  /* empty body */
	}
      *s++ = ' ';
      l++;
      for (t = srv_handle->sql_stmt; *t && l < SHM_LOG_MSG_SIZE - 1; s++, t++, l++)
	{
	  *s = *t;
	}
      *s = '\0';
    }


  gettimeofday (&exec_begin, NULL);

  ret_code =
    (*ux_exec_func) (srv_handle, flag, max_col_size, max_row, argc - bind_value_index, argv + bind_value_index, net_buf,
		     req_info, clt_cache_time_ptr, &client_cache_reusable);
  gettimeofday (&exec_end, NULL);
  ut_timeval_diff (&exec_begin, &exec_end, &elapsed_sec, &elapsed_msec);
  eid_string = get_error_log_eids (err_info.err_number);
  err_number_execute = err_info.err_number;

  if (fetch_flag && ret_code >= 0 && client_cache_reusable == FALSE)
    {
      ux_fetch (srv_handle, 1, 50, 0, 0, net_buf, req_info);
    }

  cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "%s %s%d tuple %d time %d.%03d%s%s%s", exec_func_name,
		 (ret_code < 0) ? "error:" : "", err_number_execute, get_tuple_count (srv_handle), elapsed_sec,
		 elapsed_msec, (client_cache_reusable == TRUE) ? " (CC)" : "",
		 (srv_handle->use_query_cache == true) ? " (QC)" : "", eid_string);

  query_timeout =
    ut_check_timeout (&query_start_time, &exec_end, shm_appl->long_query_time, &elapsed_sec, &elapsed_msec);
  if (query_timeout >= 0 || ret_code < 0)
    {
      if (query_timeout >= 0)
	{
	  as_info->num_long_queries %= MAX_DIAG_DATA_VALUE;
	  as_info->num_long_queries++;
	}

      if (ret_code < 0)
	{
	  update_error_query_count (as_info, &err_info);
	}

      if (as_info->cur_slow_log_mode == SLOW_LOG_MODE_ON)
	{
	  cas_slow_log_write (&query_start_time, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "%s srv_h_id %d ",
			      exec_func_name, srv_h_id);
	  if (srv_handle->sql_stmt != NULL)
	    {
	      cas_slow_log_write_query_string (srv_handle->sql_stmt, (int) strlen (srv_handle->sql_stmt));
	      bind_value_log (&query_start_time, bind_value_index, argc, argv, param_mode_size, param_mode,
			      SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), true);
	    }
	  cas_slow_log_write (NULL, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false,
			      "%s %s%d tuple %d time %d.%03d%s%s%s\n", exec_func_name, (ret_code < 0) ? "error:" : "",
			      err_number_execute,
			      (get_tuple_count (srv_handle) == INT_MAX) ? -1 : get_tuple_count (srv_handle),
			      elapsed_sec, elapsed_msec, (client_cache_reusable == TRUE) ? " (CC)" : "",
			      (srv_handle->use_query_cache == true) ? " (QC)" : "", eid_string);
	  cas_slow_log_end ();
	}
    }

  /* set is_pooled */
  if (as_info->cur_statement_pooling)
    {
      srv_handle->is_pooled = TRUE;
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_prepare_and_execute (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int prepare_argc_count;
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;

  net_arg_get_int (&prepare_argc_count, argv[0]);

  fn_prepare_internal (sock_fd, prepare_argc_count, argv + 1, net_buf, req_info, &srv_h_id);
  if (IS_ERROR_INFO_SET ())
    {
      goto prepare_and_execute_end;
    }

  /* execute argv begins at prepare argv + 1 + prepare_argc_count */
  fn_execute_internal (sock_fd, 10, argv + 1 + prepare_argc_count, net_buf, req_info, &srv_h_id);
  if (IS_ERROR_INFO_SET ())
    {
      srv_handle = hm_find_srv_handle (srv_h_id);
      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "close_req_handle srv_h_id %d", srv_h_id);
      hm_srv_handle_free (srv_h_id);
    }

prepare_and_execute_end:
  return FN_KEEP_CONN;
}

FN_RETURN
fn_set_cas_change_mode (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int mode;

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&mode, argv[0]);

  if (mode != CAS_CHANGE_MODE_AUTO && mode != CAS_CHANGE_MODE_KEEP)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_log_write (0, true, "set_cas_change_mode %s", mode == CAS_CHANGE_MODE_AUTO ? "AUTO" : "KEEP");

  ux_set_cas_change_mode (mode, net_buf);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_close_req_handle (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;
  char auto_commit_mode = FALSE;

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);

  if (argc > 1)
    {
      net_arg_get_char (auto_commit_mode, argv[1]);
    }


  srv_handle = hm_find_srv_handle (srv_h_id);
  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "close_req_handle srv_h_id %d", srv_h_id);

  hm_srv_handle_free (srv_h_id);

  net_buf_cp_int (net_buf, 0, NULL);	/* res code */

  return FN_KEEP_CONN;
}

FN_RETURN
fn_cursor (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  int offset;
  char origin;

  if (argc < 3)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);
  net_arg_get_int (&offset, argv[1]);
  net_arg_get_char (origin, argv[2]);

  ux_cursor (srv_h_id, offset, origin, net_buf);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_fetch (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  int cursor_pos;
  int fetch_count;
  int func_args;
  char fetch_flag;
  int result_set_index;
  T_SRV_HANDLE *srv_handle;

  func_args = 5;

  if (argc < func_args)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);
  net_arg_get_int (&cursor_pos, argv[1]);
  net_arg_get_int (&fetch_count, argv[2]);
  net_arg_get_char (fetch_flag, argv[3]);
  net_arg_get_int (&result_set_index, argv[4]);

  srv_handle = hm_find_srv_handle (srv_h_id);

  if (srv_handle == NULL)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);

      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "fn_fetch srv_h_id %d %s%d",
		     srv_h_id, "error:", err_info.err_number);

      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "fetch srv_h_id %d cursor_pos %d fetch_count %d",
		 srv_h_id, cursor_pos, fetch_count);

  ux_fetch (srv_handle, cursor_pos, fetch_count, fetch_flag, result_set_index, net_buf, req_info);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_schema_info (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int schema_type;
  char *arg1, *arg2;
  char flag;
  int arg1_size, arg2_size;
  int shard_id;
  int srv_h_id;

  if (argc < 4)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&schema_type, argv[0]);
  net_arg_get_str (&arg1, &arg1_size, argv[1]);
  net_arg_get_str (&arg2, &arg2_size, argv[2]);
  net_arg_get_char (flag, argv[3]);
  if (DOES_CLIENT_UNDERSTAND_THE_PROTOCOL (req_info->client_version, PROTOCOL_V5))
    {
      net_arg_get_int (&shard_id, argv[4]);
    }
  else
    {
      shard_id = 0;
    }

  cas_log_write (query_seq_num_next_value (), true, "schema_info %s %s %s %d", get_schema_type_str (schema_type),
		 (arg1 ? arg1 : "NULL"), (arg2 ? arg2 : "NULL"), flag);

  srv_h_id = ux_schema_info (schema_type, arg1, arg2, flag, net_buf, req_info, query_seq_num_current_value ());

  cas_log_write (query_seq_num_current_value (), false, "schema_info srv_h_id %d", srv_h_id);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_db_version (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  char auto_commit_mode;
  cas_log_write (0, true, "get_version");

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_char (auto_commit_mode, argv[0]);

  ux_get_db_version (net_buf, req_info);


  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }


  return FN_KEEP_CONN;
}

/* MYSQL : NOT SUPPORT MULTIPLE STATEMENT */
FN_RETURN
fn_next_result (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  char flag;
  int func_args;
  T_SRV_HANDLE *srv_handle;

  func_args = 2;

  if (argc < func_args)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);
  net_arg_get_char (flag, argv[1]);

  srv_handle = hm_find_srv_handle (srv_h_id);
  if (srv_handle == NULL)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);

      cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "fn_next_result srv_h_id %d %s%d",
		     srv_h_id, "error:", err_info.err_number);

      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "next_result %d %s", srv_h_id,
		 (srv_handle->use_query_cache == true) ? "(QC)" : "");

  ux_next_result (srv_handle, flag, net_buf, req_info);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_cursor_close (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;

  net_arg_get_int (&srv_h_id, argv[0]);

  srv_handle = hm_find_srv_handle (srv_h_id);
  if (srv_handle == NULL || srv_handle->num_q_result < 1)
    {
      /* has already been closed */
      return FN_KEEP_CONN;
    }

  cas_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), false, "cursor_close srv_h_id %d", srv_h_id);

  ux_cursor_close (srv_handle);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_con_close (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  cas_log_write (0, true, "con_close");
  net_buf_cp_int (net_buf, 0, NULL);

  if (req_info->driver_info[DRIVER_INFO_CLIENT_TYPE] != CAS_CLIENT_SERVER_SIDE_JDBC)
    {
    }
  return FN_CLOSE_CONN;
}

FN_RETURN
fn_check_cas (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int err_code = 0;

  if (argc == 1)
    {
      char *msg;
      int msg_size;
      net_arg_get_str (&msg, &msg_size, argv[0]);
      cas_log_write (0, true, "client_msg:%s", msg);
    }
  else
    {
      err_code = ux_check_connection ();
      cas_log_write (0, true, "check_cas %d", err_code);
    }

  if (err_code < 0)
    {
      ERROR_INFO_SET (err_code, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_SESS;
    }
  else
    {
      return FN_KEEP_CONN;
    }
}

FN_RETURN
fn_make_out_rs (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  return fn_not_supported (sock_fd, argc, argv, net_buf, req_info);
}

static const char *
get_schema_type_str (int schema_type)
{
  if (schema_type < 1 || schema_type > CCI_SCH_LAST)
    {
      return "";
    }

  return (schema_type_str[schema_type - 1]);
}

static const char *
get_tran_type_str (int tran_type)
{
  if (tran_type < 1 || tran_type > 2)
    {
      return "";
    }

  return (tran_type_str[tran_type - 1]);
}


static void
bind_value_log (struct timeval *log_time, int start, int argc, void **argv, int param_size, char *param_mode,
		unsigned int query_seq_num, bool slow_log)
{
  int idx;
  char type;
  int num_bind;
  void *net_value;
  const char *param_mode_str;
  void (*write2_func) (const char *, ...);

  if (slow_log)
    {
      write2_func = cas_slow_log_write2;
    }
  else
    {
      write2_func = cas_log_write2_nonl;
    }

  num_bind = 1;
  idx = start;

  while (idx < argc)
    {
      net_arg_get_char (type, argv[idx++]);
      net_value = argv[idx++];

      param_mode_str = "";
      if (param_mode != NULL && param_size >= num_bind)
	{
	  if (param_mode[num_bind - 1] == CCI_PARAM_MODE_IN)
	    param_mode_str = "(IN) ";
	  else if (param_mode[num_bind - 1] == CCI_PARAM_MODE_OUT)
	    param_mode_str = "(OUT) ";
	  else if (param_mode[num_bind - 1] == CCI_PARAM_MODE_INOUT)
	    param_mode_str = "(INOUT) ";
	}

      if (slow_log)
	{
	  cas_slow_log_write (log_time, query_seq_num, false, "bind %d %s: ", num_bind++, param_mode_str);
	}
      else
	{
	  cas_log_write_nonl (query_seq_num, false, "bind %d %s: ", num_bind++, param_mode_str);
	}

      if (type > CCI_U_TYPE_FIRST && type <= CCI_U_TYPE_LAST)
	{
	  write2_func ("%s ", type_str_tbl[(int) type]);
	  bind_value_print (type, net_value, slow_log);
	}
      else
	{
	  write2_func ("NULL");
	}
      write2_func ("\n");
    }
}


static void
bind_value_print (char type, void *net_value, bool slow_log)
{
  int data_size;
  void (*write2_func) (const char *, ...);
  void (*fwrite_func) (char *value, int size);

  if (slow_log)
    {
      write2_func = cas_slow_log_write2;
      fwrite_func = cas_slow_log_write_value_string;
    }
  else
    {
      write2_func = cas_log_write2_nonl;
      fwrite_func = cas_log_write_value_string;
    }

  net_arg_get_size (&data_size, net_value);
  if (data_size <= 0)
    {
      type = CCI_U_TYPE_NULL;
      data_size = 0;
    }

  switch (type)
    {
    case CCI_U_TYPE_CHAR:
    case CCI_U_TYPE_STRING:
    case CCI_U_TYPE_NCHAR:
    case CCI_U_TYPE_VARNCHAR:
      {
	INTL_CODESET charset = INTL_CODESET_UTF8;
	char *str_val;
	int val_size;
	int num_chars = 0;

	net_arg_get_str (&str_val, &val_size, net_value);
	if (val_size > 0)
	  {
#if defined (FOR_API_CAS)
	    num_chars = strlen (str_val);
#else
	    num_chars = intl_char_count ((const unsigned char *) str_val, val_size, charset, &num_chars) - 1;
#endif
	  }
	write2_func ("(%d)", num_chars);
	fwrite_func (str_val, val_size - 1);
      }
      break;
    case CCI_U_TYPE_BIT:
    case CCI_U_TYPE_VARBIT:
    case CCI_U_TYPE_NUMERIC:
    case CCI_U_TYPE_ENUM:
    case CCI_U_TYPE_JSON:
      {
	char *str_val;
	int val_size;
	net_arg_get_str (&str_val, &val_size, net_value);
	if (type != CCI_U_TYPE_NUMERIC)
	  {
	    write2_func ("(%d)", val_size);
	  }
	fwrite_func (str_val, val_size - 1);
      }
      break;
    case CCI_U_TYPE_BIGINT:
      {
	INT64 bi_val;
	net_arg_get_bigint (&bi_val, net_value);
	write2_func ("%lld", (long long) bi_val);
      }
      break;
    case CCI_U_TYPE_UBIGINT:
      {
	UINT64 ubi_val;
	net_arg_get_bigint ((INT64 *) (&ubi_val), net_value);
	write2_func ("%llu", (unsigned long long) ubi_val);
      }
      break;
    case CCI_U_TYPE_INT:
      {
	int i_val;
	net_arg_get_int (&i_val, net_value);
	write2_func ("%d", i_val);
      }
      break;
    case CCI_U_TYPE_UINT:
      {
	unsigned int ui_val;
	net_arg_get_int ((int *) &ui_val, net_value);
	write2_func ("%u", ui_val);
      }
      break;
    case CCI_U_TYPE_SHORT:
      {
	short s_val;
	net_arg_get_short (&s_val, net_value);
	write2_func ("%d", s_val);
      }
      break;
    case CCI_U_TYPE_USHORT:
      {
	unsigned short us_val;
	net_arg_get_short ((short *) &us_val, net_value);
	write2_func ("%u", us_val);
      }
      break;
    case CCI_U_TYPE_MONETARY:
    case CCI_U_TYPE_DOUBLE:
      {
	double d_val;
	net_arg_get_double (&d_val, net_value);
	write2_func ("%.15e", d_val);
      }
      break;
    case CCI_U_TYPE_FLOAT:
      {
	float f_val;
	net_arg_get_float (&f_val, net_value);
	write2_func ("%.6e", f_val);
      }
      break;
    case CCI_U_TYPE_DATE:
    case CCI_U_TYPE_TIME:
    case CCI_U_TYPE_TIMESTAMP:
    case CCI_U_TYPE_DATETIME:
      {
	short yr, mon, day, hh, mm, ss, ms;
	net_arg_get_datetime (&yr, &mon, &day, &hh, &mm, &ss, &ms, net_value);
	if (type == CCI_U_TYPE_DATE)
	  write2_func ("%d-%d-%d", yr, mon, day);
	else if (type == CCI_U_TYPE_TIME)
	  write2_func ("%d:%d:%d", hh, mm, ss);
	else if (type == CCI_U_TYPE_TIMESTAMP)
	  write2_func ("%d-%d-%d %d:%d:%d", yr, mon, day, hh, mm, ss);
	else
	  write2_func ("%d-%d-%d %d:%d:%d.%03d", yr, mon, day, hh, mm, ss, ms);
      }
      break;
    case CCI_U_TYPE_TIMESTAMPTZ:
    case CCI_U_TYPE_DATETIMETZ:
      {
	short yr, mon, day, hh, mm, ss, ms;
	char *tz_str_p;
	int tz_size;
	char tz_str[CCI_TZ_SIZE + 1];

	net_arg_get_datetimetz (&yr, &mon, &day, &hh, &mm, &ss, &ms, &tz_str_p, &tz_size, net_value);
	tz_size = MIN (CCI_TZ_SIZE, tz_size);
	strncpy (tz_str, tz_str_p, tz_size);
	tz_str[tz_size] = '\0';

	if (type == CCI_U_TYPE_TIMESTAMPTZ)
	  {
	    write2_func ("%d-%d-%d %d:%d:%d %s", yr, mon, day, hh, mm, ss, tz_str);
	  }
	else
	  {
	    write2_func ("%d-%d-%d %d:%d:%d.%03d %s", yr, mon, day, hh, mm, ss, ms, tz_str);
	  }
      }
      break;
    case CCI_U_TYPE_SET:
    case CCI_U_TYPE_MULTISET:
    case CCI_U_TYPE_SEQUENCE:
      {
	int remain_size = data_size;
	int ele_size;
	char ele_type;
	char *cur_p = (char *) net_value;
	char print_comma = 0;

	cur_p += 4;
	ele_type = *cur_p;
	cur_p++;
	remain_size--;

	if (ele_type <= CCI_U_TYPE_FIRST || ele_type > CCI_U_TYPE_LAST)
	  break;

	write2_func ("(%s) {", type_str_tbl[(int) ele_type]);

	while (remain_size > 0)
	  {
	    net_arg_get_size (&ele_size, cur_p);
	    if (ele_size + 4 > remain_size)
	      break;
	    if (print_comma)
	      write2_func (", ");
	    else
	      print_comma = 1;
	    bind_value_print (ele_type, cur_p, slow_log);
	    ele_size += 4;
	    cur_p += ele_size;
	    remain_size -= ele_size;
	  }

	write2_func ("}");
      }
      break;
    case CCI_U_TYPE_OBJECT:
      {
      }
      break;
    case CCI_U_TYPE_BLOB:
    case CCI_U_TYPE_CLOB:
      {
      }
      break;
    default:
      write2_func ("NULL");
      break;
    }
}

/*
 * get_error_log_eids - get error identifier string
 *    return: pointer to internal buffer
 * NOTE:
 * this function is not MT safe. Rreturned address is guaranteed to be valid
 * until next get_error_log_eids() call.
 *
 */
static char *
get_error_log_eids (int err)
{
  static char *pending_alloc = NULL;
  static char buffer[512];
  char *buf;

  if (err >= 0)
    {
      return (char *) "";
    }

  if (pending_alloc != NULL)
    {
      free (pending_alloc);
      pending_alloc = NULL;
    }

  buf = cas_log_error_handler_asprint (buffer, sizeof (buffer), true);
  if (buf != buffer)
    {
      pending_alloc = buf;
    }

  return buf;
}

FN_RETURN
fn_not_supported (SOCKET sock_fd, int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  ERROR_INFO_SET (CAS_ER_NOT_IMPLEMENTED, CAS_ERROR_INDICATOR);
  NET_BUF_ERR_SET (net_buf);
  return FN_KEEP_CONN;
}

static void
update_error_query_count (T_APPL_SERVER_INFO * as_info_p, const T_ERROR_INFO * err_info_p)
{
  assert (as_info_p != NULL);
  assert (err_info_p != NULL);

  if (err_info_p->err_number != ER_QPROC_INVALID_XASLNODE)
    {
      as_info_p->num_error_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_error_queries++;
    }

  if (err_info_p->err_indicator == DBMS_ERROR_INDICATOR)
    {
      if (err_info_p->err_number == ER_BTREE_UNIQUE_FAILED || err_info_p->err_number == ER_UNIQUE_VIOLATION_WITHKEY)
	{
	  as_info_p->num_unique_error_queries %= MAX_DIAG_DATA_VALUE;
	  as_info_p->num_unique_error_queries++;
	}
    }
}
