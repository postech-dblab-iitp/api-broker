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
 * cas_sql_log2.h -
 */

#ifndef	_CAS_SQL_LOG2_H_
#define	_CAS_SQL_LOG2_H_

#ident "$Id$"

#define SQL_LOG2_NONE		0
#define SQL_LOG2_PLAN		1
#define SQL_LOG2_HISTO		2	/* obsolete */
#define SQL_LOG2_MAX		(SQL_LOG2_PLAN | SQL_LOG2_HISTO)

#define SQL_LOG2_EXEC_BEGIN(SQL_LOG2_VALUE, STMT_ID)
#define SQL_LOG2_EXEC_END(SQL_LOG2_VALUE, STMT_ID, RES)
#define SQL_LOG2_COMPILE_BEGIN(SQL_LOG2_VALUE, SQL_STMT)
#define SQL_LOG2_EXEC_APPEND(SQL_LOG2_VALUE, STMT_ID, RES, PLAN_FILE)

extern void sql_log2_init (char *br_name, int index, int sql_log2_value, bool log_reuse_flag);
extern char *sql_log2_get_filename (void);
extern void sql_log2_dup_stdout (void);
extern void sql_log2_restore_stdout (void);
extern void sql_log2_end (bool reset_filename_flag);
extern void sql_log2_flush (void);
extern void sql_log2_write (const char *fmt, ...);
extern void sql_log2_append_file (char *file_name);

#endif /* _CAS_SQL_LOG2_H_ */
