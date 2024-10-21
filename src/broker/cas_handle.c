/*
 * Copyright 2024 CUBRID Corporation
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
 * cas_handle.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#else /* WINDOWS */
#include <unistd.h>
#endif /* WINDOWS */

#include "cas_execute.h"

#include "cas.h"
#include "cas_common.h"
#include "cas_handle.h"
#include "cas_log.h"
#include "s62ext.h"

#define SRV_HANDLE_ALLOC_SIZE		256

static void srv_handle_content_free (T_SRV_HANDLE * srv_handle);
static void col_update_info_free (T_QUERY_RESULT * q_result);
static void srv_handle_rm_tmp_file (int h_id, T_SRV_HANDLE * srv_handle);

static T_SRV_HANDLE **srv_handle_table = NULL;
static int max_srv_handle = 0;
static int max_handle_id = 0;
static int current_handle_count = 0;

/* implemented in transaction_cl.c */
extern bool tran_is_in_libcas (void);

static int current_handle_id = -1;	/* it is used for javasp */

int
hm_new_srv_handle (T_SRV_HANDLE ** new_handle, unsigned int seq_num)
{
  int i;
  int new_max_srv_handle;
  int new_handle_id = 0;
  T_SRV_HANDLE **new_srv_handle_table = NULL;
  T_SRV_HANDLE *srv_handle;

  if (cas_shard_flag == OFF && current_handle_count >= shm_appl->max_prepared_stmt_count)
    {
      return ERROR_INFO_SET (CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED, CAS_ERROR_INDICATOR);
    }


  for (i = 0; i < max_srv_handle; i++)
    {
      if (srv_handle_table[i] == NULL)
	{
	  *new_handle = srv_handle_table[i];
	  new_handle_id = i + 1;
	  break;
	}
    }

  if (new_handle_id == 0)
    {
      new_max_srv_handle = max_srv_handle + SRV_HANDLE_ALLOC_SIZE;
      new_srv_handle_table = (T_SRV_HANDLE **) REALLOC (srv_handle_table, sizeof (T_SRV_HANDLE *) * new_max_srv_handle);
      if (new_srv_handle_table == NULL)
	{
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
	}

      new_handle_id = max_srv_handle + 1;
      memset (new_srv_handle_table + max_srv_handle, 0, sizeof (T_SRV_HANDLE *) * SRV_HANDLE_ALLOC_SIZE);
      max_srv_handle = new_max_srv_handle;
      srv_handle_table = new_srv_handle_table;
    }

  srv_handle = (T_SRV_HANDLE *) MALLOC (sizeof (T_SRV_HANDLE));
  if (srv_handle == NULL)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
    }
  memset (srv_handle, 0, sizeof (T_SRV_HANDLE));
  srv_handle->id = new_handle_id;
  srv_handle->query_seq_num = seq_num;
  srv_handle->use_plan_cache = false;
  srv_handle->use_query_cache = false;
  srv_handle->is_holdable = false;
  srv_handle->is_from_current_transaction = true;
  srv_handle->is_pooled = as_info->cur_statement_pooling;

  *new_handle = srv_handle;
  srv_handle_table[new_handle_id - 1] = srv_handle;
  if (new_handle_id > max_handle_id)
    {
      max_handle_id = new_handle_id;
    }

  current_handle_count++;

  return new_handle_id;
}

T_SRV_HANDLE *
hm_find_srv_handle (int h_id)
{
  if (h_id <= 0 || h_id > max_srv_handle)
    {
      return NULL;
    }

  return (srv_handle_table[h_id - 1]);
}

void
hm_srv_handle_free (int h_id)
{
  T_SRV_HANDLE *srv_handle;

  if (h_id <= 0 || h_id > max_srv_handle)
    {
      return;
    }

  srv_handle = srv_handle_table[h_id - 1];
  if (srv_handle == NULL)
    {
      return;
    }

  srv_handle_content_free (srv_handle);
//  srv_handle_rm_tmp_file (h_id, srv_handle);

  FREE_MEM (srv_handle->classes);
  FREE_MEM (srv_handle->classes_chn);

  s62_close_statement (srv_handle->stmt_id);
  srv_handle->stmt_id = NULL;

  FREE_MEM (srv_handle);
  srv_handle_table[h_id - 1] = NULL;
  current_handle_count--;
}

void
hm_srv_handle_free_all (bool free_holdable)
{
  T_SRV_HANDLE *srv_handle;
  int i;
  int new_max_handle_id = 0;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	{
	  continue;
	}

      if (srv_handle->is_holdable && !free_holdable)
	{
	  new_max_handle_id = i;
	  continue;
	}

      srv_handle_content_free (srv_handle);
      srv_handle_rm_tmp_file (i + 1, srv_handle);

      s62_close_statement (srv_handle->stmt_id);
      srv_handle->stmt_id = NULL;

      FREE_MEM (srv_handle);
      srv_handle_table[i] = NULL;
      current_handle_count--;
    }

  max_handle_id = new_max_handle_id;
  if (free_holdable)
    {
      current_handle_count = 0;
      as_info->num_holdable_results = 0;
    }
}

void
hm_srv_handle_unset_prepare_flag_all (void)
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	{
	  continue;
	}

      srv_handle->is_prepared = FALSE;
    }

  hm_srv_handle_qresult_end_all (true);
}

void
hm_srv_handle_qresult_end_all (bool end_holdable)
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	{
	  continue;
	}

      if (srv_handle->is_holdable && !end_holdable)
	{
	  /* do not close holdable results */
	  srv_handle->is_from_current_transaction = false;
	  continue;
	}

      if (srv_handle->is_holdable && !srv_handle->is_from_current_transaction)
	{
	  /* end only holdable handles from the current transaction */
	  continue;
	}

      if (srv_handle->schema_type < 0 || srv_handle->schema_type == CCI_SCH_CLASS
	  || srv_handle->schema_type == CCI_SCH_VCLASS || srv_handle->schema_type == CCI_SCH_ATTRIBUTE
	  || srv_handle->schema_type == CCI_SCH_CLASS_ATTRIBUTE || srv_handle->schema_type == CCI_SCH_QUERY_SPEC
	  || srv_handle->schema_type == CCI_SCH_DIRECT_SUPER_CLASS || srv_handle->schema_type == CCI_SCH_PRIMARY_KEY
	  || srv_handle->schema_type == CCI_SCH_ATTR_WITH_SYNONYM)
	{
	  hm_qresult_end (srv_handle, FALSE);
	}
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
void
hm_srv_handle_set_pooled ()
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	{
	  continue;
	}

      srv_handle->is_pooled = 1;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

void
hm_qresult_clear (T_QUERY_RESULT * q_result)
{
  memset (q_result, 0, sizeof (T_QUERY_RESULT));
}

void
hm_qresult_end (T_SRV_HANDLE * srv_handle, char free_flag)
{
  T_QUERY_RESULT *q_result;
  int i;

  cas_log_write (0, true, "--hm_qresult_end");

  q_result = srv_handle->q_result;
  if (q_result)
    {
      for (i = 0; i < srv_handle->num_q_result; i++)
	{
	  if (q_result[i].copied != TRUE && q_result[i].result)
	    {
	      ux_free_result (q_result[i].result);

	      if (q_result[i].is_holdable == true)
		{
		  q_result[i].is_holdable = false;
		  as_info->num_holdable_results--;
		}
	    }
	  q_result[i].result = NULL;

	  if (q_result[i].column_info)
	    {
//            neet to clear column_info
//            db_query_format_free ((DB_QUERY_TYPE *) q_result[i].column_info);
	    }

	  q_result[i].column_info = NULL;
	  if (free_flag == TRUE)
	    {
	      col_update_info_free (&(q_result[i]));
	      FREE_MEM (q_result[i].null_type_column);
	    }
	}

      if (free_flag == TRUE)
	{
	  FREE_MEM (q_result);
	}
    }

  if (free_flag == TRUE)
    {
      srv_handle->q_result = NULL;
      srv_handle->num_q_result = 0;
      srv_handle->cur_result_index = 0;
    }

  srv_handle->cur_result = NULL;
  srv_handle->has_result_set = false;
}

void
hm_session_free (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle->session)
    {
#if !defined (FOR_API_CAS)
      db_close_session ((DB_SESSION *) (srv_handle->session));
#endif
    }
  srv_handle->session = NULL;
}

void
hm_col_update_info_clear (T_COL_UPDATE_INFO * col_update_info)
{
  memset (col_update_info, 0, sizeof (T_COL_UPDATE_INFO));
}

static void
srv_handle_content_free (T_SRV_HANDLE * srv_handle)
{
  cas_log_write (0, true, "--srv_handle_content_free [%d]", srv_handle->schema_type);

  FREE_MEM (srv_handle->sql_stmt);
//  ux_prepare_call_info_free (srv_handle->prepare_call_info);

  if (srv_handle->schema_type < 0)
    {
      hm_qresult_end (srv_handle, TRUE);
      hm_session_free (srv_handle);
    }
  else if (srv_handle->schema_type == CCI_SCH_CLASS || srv_handle->schema_type == CCI_SCH_VCLASS)
    {
      s62_close_metadata ((S62_METADATA *) srv_handle->session);
      srv_handle->session = NULL;
      srv_handle->cur_result = NULL;
    }
  else if (srv_handle->schema_type == CCI_SCH_ATTRIBUTE)
    {
      s62_close_property ((S62_PROPERTY *) srv_handle->session);
      srv_handle->session = NULL;
      srv_handle->cur_result = NULL;
    }
}

static void
col_update_info_free (T_QUERY_RESULT * q_result)
{
  int i;

  if (q_result->col_update_info)
    {
      for (i = 0; i < q_result->num_column; i++)
	{
	  FREE_MEM (q_result->col_update_info[i].attr_name);
	  FREE_MEM (q_result->col_update_info[i].class_name);
	}
      FREE_MEM (q_result->col_update_info);
    }
  q_result->col_updatable = FALSE;
  q_result->num_column = 0;
}

static void
srv_handle_rm_tmp_file (int h_id, T_SRV_HANDLE * srv_handle)
{
  if (srv_handle->query_info_flag == TRUE)
    {
      char *p;

      p = cas_log_query_plan_file (h_id);
      if (p != NULL)
	{
	  unlink (p);
	}
    }
}

int
hm_srv_handle_get_current_count (void)
{
  return current_handle_count;
}

void
hm_set_current_srv_handle (int h_id)
{
  current_handle_id = h_id;
}
