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
 * broker_monitor.c -
 */

#ident "$Id$"

#if !defined(WINDOWS)
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#include <conio.h>
#endif

#if defined(WINDOWS)
#include <sys/timeb.h>
#else
#include <sys/types.h>
#include <regex.h>
#include <sys/time.h>
#include <fcntl.h>
#include <termio.h>
#include <dlfcn.h>
#endif

#include "porting.h"
#include "cas_common.h"
#include "broker_config.h"
#include "broker_shm.h"
#include "broker_util.h"
#include "broker_process_size.h"
#include "porting.h"
#if !defined(WINDOWS)
#include "broker_process_info.h"
#endif
#include "cas_util.h"
#include "util_func.h"

#if defined (SUPPRESS_STRLEN_WARNING)
#define strlen(s1)  ((int) strlen(s1))
#endif /* defined (SUPPRESS_STRLEN_WARNING) */

#define		DEFAULT_CHECK_PERIOD		300	/* seconds */
#define		MAX_APPL_NUM		100

#define         FIELD_DELIMITER          ' '

#define         FIELD_WIDTH_BROKER_NAME 20
#define         FIELD_WIDTH_AS_ID       10

#define         BROKER_MONITOR_FLAG_MASK     0x01
#define         SHARDDB_MONITOR_FLAG_MASK    0x02
#define         PROXY_MONITOR_FLAG_MASK      0x04
#define         METADATA_MONITOR_FLAG_MASK   0x08
#define         CLIENT_MONITOR_FLAG_MASK     0x10
#define         UNUSABLE_DATABASES_FLAG_MASK 0x20

#if defined(WINDOWS) && !defined(PRId64)
#define PRId64 "lld"
#endif

typedef enum
{
  FIELD_BROKER_NAME = 0,
  FIELD_PID,
  FIELD_PSIZE,
  FIELD_PORT,
  FIELD_APPL_SERVER_NUM_TOTAL,
  FIELD_APPL_SERVER_NUM_CLIENT_WAIT,
  FIELD_APPL_SERVER_NUM_BUSY,
  FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC,
  FIELD_APPL_SERVER_NUM_BUSY_IN_SEC,
  FIELD_JOB_QUEUE_ID,
  FIELD_THREAD,			/* = 10 */
  FIELD_CPU_USAGE,
  FIELD_CPU_TIME,
  FIELD_TPS,
  FIELD_QPS,
  FIELD_NUM_OF_SELECT_QUERIES,
  FIELD_NUM_OF_INSERT_QUERIES,
  FIELD_NUM_OF_UPDATE_QUERIES,
  FIELD_NUM_OF_DELETE_QUERIES,
  FIELD_NUM_OF_OTHERS_QUERIES,
  FIELD_LONG_TRANSACTION,	/* = 20 */
  FIELD_LONG_QUERY,
  FIELD_ERROR_QUERIES,
  FIELD_UNIQUE_ERROR_QUERIES,
  FIELD_CANCELED,
  FIELD_ACCESS_MODE,
  FIELD_SQL_LOG,
  FIELD_NUMBER_OF_CONNECTION,
  FIELD_ID,
  FIELD_LQS,
  FIELD_STATUS,			/* = 30 */
  FIELD_LAST_ACCESS_TIME,
  FIELD_DB_NAME,
  FIELD_HOST,
  FIELD_LAST_CONNECT_TIME,
  FIELD_CLIENT_IP,
  FIELD_CLIENT_VERSION,
  FIELD_SQL_LOG_MODE,
  FIELD_TRANSACTION_STIME,
  FIELD_CONNECT,
  FIELD_RESTART,		/* = 40 */
  FIELD_REQUEST,
  FIELD_SHARD_ID,
  FIELD_PROXY_ID,
  FIELD_SHARD_Q_SIZE,
  FIELD_STMT_POOL_RATIO,
  FIELD_NUMBER_OF_CONNECTION_REJECTED,
  FIELD_UNUSABLE_DATABASES,
  FIELD_LAST = FIELD_UNUSABLE_DATABASES
} FIELD_NAME;

typedef enum
{
  FIELD_T_STRING = 0,
  FIELD_T_INT,
  FIELD_T_FLOAT,
  FIELD_T_UINT64,
  FIELD_T_INT64,
  FIELD_T_TIME
} FIELD_TYPE;

typedef enum
{
  MONITOR_T_BROKER = 0,
  MONITOR_T_SHARDDB,
  MONITOR_T_PROXY,
  MONITOR_T_LAST = MONITOR_T_PROXY
} MONITOR_TYPE;

typedef enum
{
  FIELD_LEFT_ALIGN = 0,
  FIELD_RIGHT_ALIGN
} FIELD_ALIGN;

struct status_field
{
  FIELD_NAME name;
  unsigned int width;
  char title[256];
  FIELD_ALIGN align;
};

struct status_field fields[FIELD_LAST + 1] = {
  {FIELD_BROKER_NAME, FIELD_WIDTH_BROKER_NAME, "NAME", FIELD_LEFT_ALIGN},
  {FIELD_PID, 5, "PID", FIELD_RIGHT_ALIGN},
  {FIELD_PSIZE, 7, "PSIZE", FIELD_RIGHT_ALIGN},
  {FIELD_PORT, 5, "PORT", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_TOTAL, 5, "", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_CLIENT_WAIT, 6, "W", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_BUSY, 6, "B", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC, 6, "", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_BUSY_IN_SEC, 6, "", FIELD_RIGHT_ALIGN},
  {FIELD_JOB_QUEUE_ID, 4, "JQ", FIELD_RIGHT_ALIGN},
  {FIELD_THREAD, 4, "THR", FIELD_RIGHT_ALIGN},
  {FIELD_CPU_USAGE, 6, "CPU", FIELD_RIGHT_ALIGN},
  {FIELD_CPU_TIME, 6, "CTIME", FIELD_RIGHT_ALIGN},
  {FIELD_TPS, 20, "TPS", FIELD_RIGHT_ALIGN},
  {FIELD_QPS, 20, "QPS", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_SELECT_QUERIES, 8, "SELECT", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_INSERT_QUERIES, 8, "INSERT", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_UPDATE_QUERIES, 8, "UPDATE", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_DELETE_QUERIES, 8, "DELETE", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_OTHERS_QUERIES, 8, "OTHERS", FIELD_RIGHT_ALIGN},
  /*
   * 5: width of long transaction count
   * 1: delimiter(/)
   * 4: width of long transaction time
   * output example :
   *    [long transaction count]/[long transaction time]
   *    10/60.0
   * */
  {FIELD_LONG_TRANSACTION, 5 + 1 + 4, "LONG-T", FIELD_RIGHT_ALIGN},
  /*
   * 5: width of long query count
   * 1: delimiter(/)
   * 4: width of long query time
   * output example :
   *    [long query count]/[long query time]
   *    10/60.0
   * */
  {FIELD_LONG_QUERY, 5 + 1 + 4, "LONG-Q", FIELD_RIGHT_ALIGN},
  {FIELD_ERROR_QUERIES, 13, "ERR-Q", FIELD_RIGHT_ALIGN},
  {FIELD_UNIQUE_ERROR_QUERIES, 13, "UNIQUE-ERR-Q", FIELD_RIGHT_ALIGN},
  {FIELD_CANCELED, 10, "CANCELED", FIELD_RIGHT_ALIGN},
  {FIELD_ACCESS_MODE, 13, "ACCESS_MODE", FIELD_RIGHT_ALIGN},
  {FIELD_SQL_LOG, 9, "SQL_LOG", FIELD_RIGHT_ALIGN},
  {FIELD_NUMBER_OF_CONNECTION, 9, "#CONNECT", FIELD_RIGHT_ALIGN},
  {FIELD_ID, FIELD_WIDTH_AS_ID, "ID", FIELD_RIGHT_ALIGN},
  {FIELD_LQS, 10, "LQS", FIELD_RIGHT_ALIGN},
  {FIELD_STATUS, 12, "STATUS", FIELD_LEFT_ALIGN},
  {FIELD_LAST_ACCESS_TIME, 19, "LAST ACCESS TIME", FIELD_RIGHT_ALIGN},
  {FIELD_DB_NAME, 16, "DB", FIELD_RIGHT_ALIGN},
  {FIELD_HOST, 16, "HOST", FIELD_RIGHT_ALIGN},
  {FIELD_LAST_CONNECT_TIME, 19, "LAST CONNECT TIME", FIELD_RIGHT_ALIGN},
  {FIELD_CLIENT_IP, 15, "CLIENT IP", FIELD_RIGHT_ALIGN},
  {FIELD_CLIENT_VERSION, 19, "CLIENT VERSION", FIELD_RIGHT_ALIGN},
  {FIELD_SQL_LOG_MODE, 15, "SQL_LOG_MODE", FIELD_RIGHT_ALIGN},
  {FIELD_TRANSACTION_STIME, 19, "TRANSACTION STIME", FIELD_RIGHT_ALIGN},
  {FIELD_CONNECT, 9, "#CONNECT", FIELD_RIGHT_ALIGN},
  {FIELD_RESTART, 9, "#RESTART", FIELD_RIGHT_ALIGN},
  {FIELD_REQUEST, 20, "#REQUEST", FIELD_RIGHT_ALIGN},
  {FIELD_SHARD_ID, 10, "SHARD_ID", FIELD_RIGHT_ALIGN},
  {FIELD_PROXY_ID, 10, "PROXY_ID", FIELD_RIGHT_ALIGN},
  {FIELD_SHARD_Q_SIZE, 7, "SHARD-Q", FIELD_RIGHT_ALIGN},
  {FIELD_STMT_POOL_RATIO, 20, "STMT-POOL-RATIO(%)", FIELD_RIGHT_ALIGN},
  {FIELD_NUMBER_OF_CONNECTION_REJECTED, 9, "#REJECT", FIELD_RIGHT_ALIGN},
  {FIELD_UNUSABLE_DATABASES, 100, "UNUSABLE_DATABASES", FIELD_LEFT_ALIGN}
};

/* structure for appl monitoring */
typedef struct appl_monitoring_item APPL_MONITORING_ITEM;
struct appl_monitoring_item
{
  INT64 num_query_processed;
  INT64 num_long_query;
  INT64 qps;
  INT64 lqs;
};

/* structure for broker monitoring */
typedef struct br_monitoring_item BR_MONITORING_ITEM;
struct br_monitoring_item
{
  UINT64 num_tx;
  UINT64 num_qx;
  UINT64 num_lt;
  UINT64 num_lq;
  UINT64 num_eq;
  UINT64 num_eq_ui;
  UINT64 num_interrupt;
  UINT64 tps;
  UINT64 qps;
  UINT64 lts;
  UINT64 lqs;
  UINT64 eqs_ui;
  UINT64 eqs;
  UINT64 its;
  UINT64 num_select_query;
  UINT64 num_insert_query;
  UINT64 num_update_query;
  UINT64 num_delete_query;
  UINT64 num_others_query;
  UINT64 num_interrupts;
  UINT64 num_request;
  UINT64 num_connect;
  UINT64 num_connect_reject;
  UINT64 num_client_wait;
  UINT64 num_client_wait_nsec;
  UINT64 num_busy;
  UINT64 num_busy_nsec;
  UINT64 num_restart;
  UINT64 shard_waiter_count;
  UINT64 num_request_stmt;
  UINT64 num_request_stmt_in_pool;
  int num_appl_server;
};

typedef struct shard_stat_item SHARD_STAT_ITEM;
struct shard_stat_item
{
  int shard_id;

  INT64 num_hint_key_queries_requested;
  INT64 num_hint_id_queries_requested;
  INT64 num_no_hint_queries_requested;
};

typedef struct key_stat_item KEY_STAT_ITEM;
struct key_stat_item
{
  INT64 num_range_queries_requested[SHARD_KEY_RANGE_MAX];
};

static void str_to_screen (const char *msg);
static void print_newline ();
static int get_char (void);
static void print_usage (void);
static int get_args (int argc, char *argv[], char *br_vector);
static void print_job_queue (T_MAX_HEAP_NODE *);
static void ip2str (unsigned char *ip, char *ip_str);
static void time2str (const time_t t, char *str);

static void print_monitor_header (MONITOR_TYPE mnt_type);
static void set_monitor_items (BR_MONITORING_ITEM * mnt_items, T_BROKER_INFO * br_info, T_SHM_APPL_SERVER * shm_appl,
			       T_SHM_PROXY * shm_proxy, MONITOR_TYPE mnt_type);
static void print_monitor_items (BR_MONITORING_ITEM * mnt_items_cur, BR_MONITORING_ITEM * mnt_items_old,
				 int num_mnt_items, double elapsed_time, T_BROKER_INFO * br_info_p,
				 T_SHM_APPL_SERVER * shm_appl, MONITOR_TYPE mnt_type);

static void appl_info_display (T_SHM_APPL_SERVER * shm_appl, T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index,
			       APPL_MONITORING_ITEM * appl_mnt_old, time_t current_time, double elapsed_time);
static int appl_monitor (char *br_vector, double elapsed_time);
static int brief_monitor (char *br_vector, MONITOR_TYPE mnt_type, double elapsed_time);

#ifdef GET_PSINFO
static void time_format (int t, char *time_str);
#endif
static void print_appl_header (bool use_pdh_flag);
static int print_title (char *buf_p, int buf_offset, FIELD_NAME name, const char *new_title_p);
static void print_value (FIELD_NAME name, const void *value, FIELD_TYPE type);
static int get_num_monitor_items (MONITOR_TYPE mnt_type, T_SHM_PROXY * shm_proxy_p);
static const char *get_access_mode_string (T_ACCESS_MODE_VALUE mode, int replica_only_flag);
static const char *get_sql_log_mode_string (T_SQL_LOG_MODE_VALUE mode);
static const char *get_status_string (T_APPL_SERVER_INFO * as_info_p, char appl_server);
static void get_cpu_usage_string (char *buf_p, float usage);


static void move (int x, int y);
static void refresh ();
static void clear ();
static void clrtobot ();
static void clrtoeol ();
static void endwin ();

#if !defined (WINDOWS)
#define TINFO_HIGH_VERSION      10
#define TINFO_LOW_VERSION       5

static int getch ();
static void *initscr ();
static void noecho ();
static void timeout (int delay);
static void addstr (const char *);
static int tty_noblock (void);
static int get_timeout (void);
static int num_newlines (const char *);
char *term = NULL;

typedef char *(*tgoto_func_t) (const char *cap, int col, int row);
typedef int (*tgetent_func_t) (char *bp, const char *name);
typedef char *(*tgetstr_func_t) (char *id, char **area);
typedef int (*tputs_func_t) (const char *str, int affcnt, int (*putc) (int));
typedef int (*tgetnum_func_t) (char *id);

tgoto_func_t tgoto;
tgetent_func_t tgetent;
tgetstr_func_t tgetstr;
tputs_func_t tputs;
tgetnum_func_t tgetnum;

int Stdin_timer = 0;
static int currentY = 0;
static int tty_Lines = 0;
static int tty_Cols = 0;
static int num_Chars = 0;
static char *cm = NULL;
static char *cd = NULL;
static char *ce = NULL;
static char *cl = NULL;

void *dl_handle = NULL;

struct termios oterm;
#endif

static T_SHM_BROKER *shm_br;
static bool display_job_queue = false;
static int refresh_sec = 0;
static int last_access_sec = 0;
static bool tty_mode = false;
static bool full_info_flag = false;
static int state_interval = 1;
static char service_filter_value = SERVICE_UNKNOWN;

static unsigned int monitor_flag = 0;

#if defined(WINDOWS)
HANDLE h_console;
CONSOLE_SCREEN_BUFFER_INFO scr_info;
#endif
static void
str_to_screen (const char *msg)
{
#ifdef WINDOWS
  DWORD size;
  (void) WriteConsole (h_console, msg, strlen (msg), &size, NULL);
#else
  (void) addstr (msg);
#endif
}

static void
str_out (const char *fmt, ...)
{
  va_list ap;
  char out_buf[1024];

  va_start (ap, fmt);
  if (refresh_sec > 0 && !tty_mode)
    {
      vsprintf (out_buf, fmt, ap);
      str_to_screen (out_buf);
    }
  else
    {
      vprintf (fmt, ap);
    }
  va_end (ap);
}

static void
print_newline ()
{
  if (refresh_sec > 0 && !tty_mode)
    {
      clrtoeol ();
      str_to_screen ("\n");
    }
  else
    {
      printf ("\n");
    }
}

static int
get_char (void)
{
#ifdef WINDOWS
  int i;
  for (i = 0; i < refresh_sec * 10; i++)
    {
      if (shm_br != NULL && shm_br->magic == 0)
	{
	  return 0;
	}

      if (_kbhit ())
	{
	  return _getch ();
	}
      else
	{
	  SLEEP_MILISEC (0, 100);
	}
    }
  return 0;
#else
  return getch ();
#endif
}


int
main (int argc, char **argv)
{
  T_BROKER_INFO br_info[MAX_BROKER_NUM];
  int num_broker, master_shm_id;
  int err, i;
  char *br_vector;
#if defined(WINDOWS)
#else
  void *win;
#endif
  time_t time_old, time_cur;
  double elapsed_time;

  if (argc == 2 && strcmp (argv[1], "--version") == 0)
    {
      fprintf (stderr, "VERSION %s\n", makestring (BUILD_NUMBER));
      return 3;
    }

  err = broker_config_read (NULL, br_info, &num_broker, &master_shm_id, NULL, 0, NULL, NULL, NULL);
  if (err < 0)
    {
      return 2;
    }

  ut_cd_work_dir ();

  shm_br = (T_SHM_BROKER *) uw_shm_open (master_shm_id, SHM_BROKER, SHM_MODE_MONITOR);
  if (shm_br == NULL)
    {
      /* This means we have to launch broker */
      fprintf (stdout, "master shared memory open error[0x%x]\r\n", master_shm_id);
      return 1;
    }
  if (shm_br->num_broker < 1 || shm_br->num_broker > MAX_BROKER_NUM)
    {
      PRINT_AND_LOG_ERR_MSG ("broker configuration error\r\n");
      return 3;
    }

  br_vector = (char *) malloc (shm_br->num_broker);
  if (br_vector == NULL)
    {
      PRINT_AND_LOG_ERR_MSG ("memory allocation error\r\n");
      return 3;
    }
  for (i = 0; i < shm_br->num_broker; i++)
    {
      br_vector[i] = 0;
    }

  if (get_args (argc, argv, br_vector) < 0)
    {
      free (br_vector);
      return 3;
    }

  if (refresh_sec > 0 && !tty_mode)
    {
#if defined(WINDOWS)
      h_console = GetStdHandle (STD_OUTPUT_HANDLE);
      if (h_console == NULL)
	{
	  refresh_sec = 0;
	}
      if (!GetConsoleScreenBufferInfo (h_console, &scr_info))
	{
	  scr_info.dwSize.X = 80;
	  scr_info.dwSize.Y = 50;
	}
//  FillConsoleOutputCharacter(h_console, ' ', scr_info.dwSize.X * scr_info.dwSize.Y, top_left_pos, &size);
#else
      if ((win = initscr ()) == NULL)
	{
	  if (dl_handle != NULL)
	    {
	      dlclose (dl_handle);
	      dl_handle = NULL;
	    }
	  fprintf (stderr, "fail to initialize tinfo library\n");
	  return 127;
	}

      timeout (refresh_sec * 1000);
      noecho ();
#endif
    }

  (void) time (&time_old);
  time_old--;

  while (1)
    {
      (void) time (&time_cur);
      elapsed_time = difftime (time_cur, time_old);

      if (refresh_sec > 0 && !tty_mode)
	{
	  move (0, 0);
	  refresh ();
	}

      if (shm_br == NULL || shm_br->magic == 0)
	{
	  if (shm_br)
	    {
	      uw_shm_detach (shm_br);
	    }

	  shm_br = (T_SHM_BROKER *) uw_shm_open (master_shm_id, SHM_BROKER, SHM_MODE_MONITOR);
	}
      else
	{
	  if (monitor_flag & BROKER_MONITOR_FLAG_MASK)
	    {
	      if ((monitor_flag & ~BROKER_MONITOR_FLAG_MASK) != 0)
		{
		  print_newline ();
		}
	      brief_monitor (br_vector, MONITOR_T_BROKER, elapsed_time);
	    }

	  if (monitor_flag & SHARDDB_MONITOR_FLAG_MASK)
	    {
	      if ((monitor_flag & ~SHARDDB_MONITOR_FLAG_MASK) != 0)
		{
		  print_newline ();
		  str_out ("<SHARD INFO>");
		  print_newline ();
		}
	      brief_monitor (br_vector, MONITOR_T_SHARDDB, elapsed_time);
	    }

	  if (monitor_flag & PROXY_MONITOR_FLAG_MASK)
	    {
	      if ((monitor_flag & ~PROXY_MONITOR_FLAG_MASK) != 0)
		{
		  print_newline ();
		  str_out ("<PROXY INFO>");
		  print_newline ();
		}
	      brief_monitor (br_vector, MONITOR_T_PROXY, elapsed_time);
	    }

	  if (monitor_flag == 0)
	    {
	      appl_monitor (br_vector, elapsed_time);
	    }
	}

      if (refresh_sec > 0 && !tty_mode)
	{
	  int in_ch = 0;

	  refresh ();
	  clrtobot ();
	  move (0, 0);
	  refresh ();
	  in_ch = get_char ();

#if defined (WINDOWS)
	  if (shm_br != NULL && shm_br->magic == 0)
	    {
	      uw_shm_detach (shm_br);
	      shm_br = NULL;
	    }
#endif
	  if (in_ch == 'q')
	    {
	      break;
	    }
	  else if (in_ch == '' || in_ch == '\r' || in_ch == '\n' || in_ch == ' ')
	    {
	      clear ();
	      refresh ();
	    }
	}
      else if (refresh_sec > 0)
	{
	  for (i = 0; i < 10; i++)
	    {
#if defined (WINDOWS)
	      if (shm_br != NULL && shm_br->magic == 0)
		{
		  uw_shm_detach (shm_br);
		  shm_br = NULL;
		}
#endif
	      SLEEP_MILISEC (0, refresh_sec * 100);
	    }
	  fflush (stdout);
	}
      else
	{
	  break;
	}

      if (elapsed_time > 0)
	{
	  time_old = time_cur;
	}
    }				/* end of while(1) */

  if (shm_br != NULL)
    {
      uw_shm_detach (shm_br);
    }

  if (refresh_sec > 0 && !tty_mode)
    {
      endwin ();
    }

  return 0;
}

static void
print_usage (void)
{
  printf ("broker_monitor [-b] [-q] [-t] [-s <sec>] [-c] [-u] [-f] [<expr>]\n");
  printf ("\t<expr> part of gateway name or SERVICE=[ON|OFF]\n");
  printf ("\t-q display job queue\n");
//  printf ("\t-c display client information\n");
//  printf ("\t-u display unusable database server\n");
  printf ("\t-b brief mode (show gateway info)\n");
  printf ("\t-s refresh time in sec\n");
  printf ("\t-f full info\n");
}

static int
get_args (int argc, char *argv[], char *br_vector)
{
  int c, j;
  int status;
  bool br_name_opt_flag = false;
#if !defined(WINDOWS)
  regex_t re;
#endif

  char optchars[] = "hbqts:l:fmcSPu";

  display_job_queue = false;
  refresh_sec = 0;
  last_access_sec = 0;
  full_info_flag = false;
  state_interval = 1;
  service_filter_value = SERVICE_UNKNOWN;
  while ((c = getopt (argc, argv, optchars)) != EOF)
    {
      switch (c)
	{
	case 't':
	  tty_mode = true;
	  break;
	case 'q':
	  display_job_queue = true;
	  break;
	case 's':
	  refresh_sec = atoi (optarg);
	  break;
	case 'b':
	  monitor_flag |= BROKER_MONITOR_FLAG_MASK;
	  break;
	case 'l':
	  state_interval = last_access_sec = atoi (optarg);
	  if (state_interval < 1)
	    {
	      state_interval = 1;
	    }
	  break;
	case 'f':
	  full_info_flag = true;
	  break;
	case 'h':
	case '?':
	  print_usage ();
	  return -1;
	}
    }

  for (; optind < argc; optind++)
    {
      if (br_name_opt_flag == false)
	{
	  if (strncasecmp (argv[optind], "SERVICE=", strlen ("SERVICE=")) == 0)
	    {
	      char *value_p;
	      value_p = argv[optind] + strlen ("SERVICE=");
	      if (strcasecmp (value_p, "ON") == 0)
		{
		  service_filter_value = SERVICE_ON;
		  break;
		}
	      else if (strcasecmp (value_p, "OFF") == 0)
		{
		  service_filter_value = SERVICE_OFF;
		  break;
		}
	      else
		{
		  print_usage ();
		  return -1;
		}
	    }
	}

      br_name_opt_flag = true;
#if !defined(WINDOWS)
      if (regcomp (&re, argv[optind], 0) != 0)
	{
	  fprintf (stderr, "%s\r\n", argv[optind]);
	  return -1;
	}
#endif
      for (j = 0; j < shm_br->num_broker; j++)
	{
#if defined(WINDOWS)
	  status = (strstr (shm_br->br_info[j].name, argv[optind]) != NULL) ? 0 : 1;
#else
	  status = regexec (&re, shm_br->br_info[j].name, 0, NULL, 0);
#endif
	  if (status == 0)
	    {
	      br_vector[j] = 1;
	    }
	}
#if !defined(WINDOWS)
      regfree (&re);
#endif
    }

  if (br_name_opt_flag == false)
    {
      for (j = 0; j < shm_br->num_broker; j++)
	br_vector[j] = 1;
    }

  return 0;
}

static void
print_job_queue (T_MAX_HEAP_NODE * job_queue)
{
  T_MAX_HEAP_NODE item;
  char first_flag = 1;
  char outbuf[1024];

  while (1)
    {
      char ip_str[64];
      char time_str[64];

      if (max_heap_delete (job_queue, &item) < 0)
	break;

      if (first_flag)
	{
	  sprintf (outbuf, "%5s  %s%9s%13s%13s", "ID", "PRIORITY", "IP", "TIME", "REQUEST");
	  str_out ("%s", outbuf);
	  print_newline ();
	  first_flag = 0;
	}

      ip2str (item.ip_addr, ip_str);
      time2str (item.recv_time, time_str);
      sprintf (outbuf, "%5d%7d%17s%10s   %s:%s", item.id, item.priority, ip_str, time_str, item.script, item.prg_name);
      str_out ("%s", outbuf);
      print_newline ();
    }
  if (!first_flag)
    print_newline ();
}

static void
ip2str (unsigned char *ip, char *ip_str)
{
  sprintf (ip_str, "%d.%d.%d.%d", (unsigned char) ip[0], (unsigned char) ip[1], (unsigned char) ip[2],
	   (unsigned char) ip[3]);
}

static void
time2str (const time_t t, char *str)
{
  struct tm s_tm;

  if (localtime_r (&t, &s_tm) == NULL)
    {
      *str = '\0';
      return;
    }
  sprintf (str, "%02d:%02d:%02d", s_tm.tm_hour, s_tm.tm_min, s_tm.tm_sec);
}

static void
appl_info_display (T_SHM_APPL_SERVER * shm_appl, T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index,
		   APPL_MONITORING_ITEM * appl_mnt_old, time_t current_time, double elapsed_time)
{
  UINT64 qps;
  UINT64 lqs;
  int col_len;
  time_t tran_start_time;
  char ip_str[16];
  int as_id;
  int proxy_id = 0;
#if !defined (WINDOWS)
  int psize;
#endif
#if defined (GET_PSINFO) || defined (WINDOWS)
  char buf[256];
#endif

  {
    as_id = as_index + 1;
  }

  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }

  if (last_access_sec > 0)
    {
      if (as_info_p->uts_status != UTS_STATUS_BUSY || current_time - as_info_p->last_access_time < last_access_sec)
	{
	  return;
	}

      if (as_info_p->uts_status == UTS_STATUS_BUSY && IS_APPL_SERVER_TYPE_CAS (shm_br->br_info[br_index].appl_server)
	  && as_info_p->con_status == CON_STATUS_OUT_TRAN)
	{
	  return;
	}
    }

  col_len = 0;

  {
    print_value (FIELD_ID, &as_id, FIELD_T_INT);
  }
  print_value (FIELD_PID, &as_info_p->pid, FIELD_T_INT);
  if (elapsed_time > 0)
    {
      qps = (as_info_p->num_queries_processed - appl_mnt_old->num_query_processed) / elapsed_time;
      lqs = (as_info_p->num_long_queries - appl_mnt_old->num_long_query) / elapsed_time;
      appl_mnt_old->num_query_processed = as_info_p->num_queries_processed;
      appl_mnt_old->num_long_query = as_info_p->num_long_queries;
      appl_mnt_old->qps = qps;
      appl_mnt_old->lqs = lqs;
    }
  else
    {
      qps = appl_mnt_old->qps;
      lqs = appl_mnt_old->lqs;
    }

  print_value (FIELD_QPS, &qps, FIELD_T_UINT64);
  print_value (FIELD_LQS, &lqs, FIELD_T_UINT64);
#if defined(WINDOWS)
  print_value (FIELD_PORT, &(as_info_p->as_port), FIELD_T_INT);
#endif
#if defined(WINDOWS)
  if (shm_appl->use_pdh_flag == TRUE)
    {
      print_value (FIELD_PSIZE, &(as_info_p->pdh_workset), FIELD_T_INT);
    }
#else
  psize = getsize (as_info_p->pid);
  print_value (FIELD_PSIZE, &psize, FIELD_T_INT);
#endif
  print_value (FIELD_STATUS, get_status_string (as_info_p, shm_br->br_info[br_index].appl_server), FIELD_T_STRING);

#ifdef GET_PSINFO
  get_psinfo (as_info_p->pid, &proc_info);

  get_cpu_usage_string (buf, proc_info.pcpu);
  print_value (FIELD_CPU_USAGE, buf, FIELD_T_STRING);

  time_format (proc_info.cpu_time, time_str);
  print_value (FIELD_CPU_TIME, time_str, FIELD_T_STRING);
#elif WINDOWS
  if (shm_appl->use_pdh_flag == TRUE)
    {
      get_cpu_usage_string (buf, as_info_p->pdh_pct_cpu);
      print_value (FIELD_CPU_USAGE, buf, FIELD_T_STRING);
    }
#endif

  if (full_info_flag)
    {
      print_value (FIELD_LAST_ACCESS_TIME, &(as_info_p->last_access_time), FIELD_T_TIME);
      if (as_info_p->database_name[0] != '\0')
	{
	  print_value (FIELD_DB_NAME, as_info_p->database_name, FIELD_T_STRING);
	  print_value (FIELD_HOST, as_info_p->database_host, FIELD_T_STRING);
	  print_value (FIELD_LAST_CONNECT_TIME, &(as_info_p->last_connect_time), FIELD_T_TIME);
	}
      else
	{
	  print_value (FIELD_DB_NAME, (char *) "-", FIELD_T_STRING);
	  print_value (FIELD_HOST, (char *) "-", FIELD_T_STRING);
	  print_value (FIELD_LAST_CONNECT_TIME, (char *) "-", FIELD_T_STRING);
	}

      print_value (FIELD_CLIENT_IP, ut_get_ipv4_string (ip_str, sizeof (ip_str), as_info_p->cas_clt_ip),
		   FIELD_T_STRING);
      print_value (FIELD_CLIENT_VERSION, as_info_p->driver_version, FIELD_T_STRING);

      if (as_info_p->cur_sql_log_mode != shm_appl->sql_log_mode)
	{
	  print_value (FIELD_SQL_LOG_MODE, get_sql_log_mode_string ((T_SQL_LOG_MODE_VALUE) as_info_p->cur_sql_log_mode),
		       FIELD_T_STRING);
	}
      else
	{
	  print_value (FIELD_SQL_LOG_MODE, (char *) "-", FIELD_T_STRING);
	}

      tran_start_time = as_info_p->transaction_start_time;
      if (tran_start_time != (time_t) 0)
	{
	  print_value (FIELD_TRANSACTION_STIME, &tran_start_time, FIELD_T_TIME);
	}
      else
	{
	  print_value (FIELD_TRANSACTION_STIME, (char *) "-", FIELD_T_STRING);
	}
      print_value (FIELD_CONNECT, &(as_info_p->num_connect_requests), FIELD_T_INT);
      print_value (FIELD_RESTART, &(as_info_p->num_restarts), FIELD_T_INT);
    }
  print_newline ();
  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      str_out ("SQL: %s", as_info_p->log_msg);
      print_newline ();
    }
}

static int
appl_monitor (char *br_vector, double elapsed_time)
{
  T_MAX_HEAP_NODE job_queue[JOB_QUEUE_MAX_SIZE + 1];
  T_SHM_APPL_SERVER *shm_appl;
  int i, j, k, appl_offset;

  static APPL_MONITORING_ITEM *appl_mnt_olds = NULL;

  time_t current_time;
#ifdef GET_PSINFO
  T_PSINFO proc_info;
  char time_str[32];
#endif

  if (appl_mnt_olds == NULL)
    {
      int n = 0;
      for (i = 0; i < shm_br->num_broker; i++)
	{
	  n += shm_br->br_info[i].appl_server_max_num;
	}

      appl_mnt_olds = (APPL_MONITORING_ITEM *) calloc (sizeof (APPL_MONITORING_ITEM), n);
      if (appl_mnt_olds == NULL)
	{
	  return -1;
	}
      memset ((char *) appl_mnt_olds, 0, sizeof (APPL_MONITORING_ITEM) * n);
    }

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (br_vector[i] == 0)
	{
	  continue;
	}

      if (service_filter_value != SERVICE_UNKNOWN && service_filter_value != shm_br->br_info[i].service_flag)
	{
	  continue;
	}

      str_out ("%% %s", shm_br->br_info[i].name);

      if (shm_br->br_info[i].service_flag == SERVICE_ON)
	{
	  shm_appl =
	    (T_SHM_APPL_SERVER *) uw_shm_open (shm_br->br_info[i].appl_server_shm_id, SHM_APPL_SERVER,
					       SHM_MODE_MONITOR);
	  if (shm_appl == NULL)
	    {
	      str_out ("%s", "shared memory open error");
	      print_newline ();
	    }
	  else
	    {
	      print_newline ();
#if defined (WINDOWS)
	      print_appl_header (shm_appl->use_pdh_flag);
#else
	      print_appl_header (false);
#endif
	      current_time = time (NULL);

	      /* CAS INFORMATION DISPLAY */
	      appl_offset = 0;

	      for (k = 0; k < i; k++)
		{
		  appl_offset += shm_br->br_info[k].appl_server_max_num;
		}
	      for (j = 0; j < shm_br->br_info[i].appl_server_max_num; j++)
		{
		  appl_info_display (shm_appl, &(shm_appl->as_info[j]), i, j, &(appl_mnt_olds[appl_offset + j]),
				     current_time, elapsed_time);
		}		/* CAS INFORMATION DISPLAY */

	      print_newline ();

	      if (display_job_queue == true)
		{
		  print_job_queue (job_queue);
		}

	      if (shm_appl)
		{
		  uw_shm_detach (shm_appl);
		}
	    }
	}

      else
	{			/* service_flag == OFF */
	  str_out ("%c%s", FIELD_DELIMITER, "OFF");
	  print_newline ();
	  print_newline ();
	}
    }

  return 0;
}

static void
print_monitor_header (MONITOR_TYPE mnt_type)
{
  char buf[LINE_MAX];
  int buf_offset = 0;
  int i;
  static unsigned int tty_print_header = 0;

  assert (mnt_type <= MONITOR_T_LAST);

  if (tty_mode == true && (tty_print_header++ % 20 != 0) && mnt_type == MONITOR_T_BROKER
      && (monitor_flag & ~BROKER_MONITOR_FLAG_MASK) == 0)
    {
      return;
    }

  buf_offset = 0;

  if (mnt_type == MONITOR_T_BROKER)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_BROKER_NAME, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_PID, NULL);
      if (full_info_flag)
	{
	  buf_offset = print_title (buf, buf_offset, FIELD_PSIZE, NULL);
	}
      buf_offset = print_title (buf, buf_offset, FIELD_PORT, NULL);
    }
  else if (mnt_type == MONITOR_T_SHARDDB)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_SHARD_ID, NULL);
    }
  else if (mnt_type == MONITOR_T_PROXY)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_PROXY_ID, NULL);
    }

  if (full_info_flag)
    {
      char field_title_with_interval[256];

      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_TOTAL, (char *) "AS(T");
      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_CLIENT_WAIT, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_BUSY, NULL);
      sprintf (field_title_with_interval, "%d%s", state_interval, "s-W");
      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC, field_title_with_interval);
      sprintf (field_title_with_interval, "%d%s", state_interval, "s-B)");
      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_BUSY_IN_SEC, field_title_with_interval);
    }
  else
    {
      buf_offset = print_title (buf, buf_offset, FIELD_APPL_SERVER_NUM_TOTAL, (char *) "AS");
    }

  if (mnt_type == MONITOR_T_BROKER)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_JOB_QUEUE_ID, NULL);
    }
  else if (mnt_type == MONITOR_T_SHARDDB || mnt_type == MONITOR_T_PROXY)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_SHARD_Q_SIZE, NULL);
    }

#ifdef GET_PSINFO
  buf_offset = print_title (buf, buf_offset, FIELD_THREAD, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_USAGE, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_TIME, NULL);
#endif

  buf_offset = print_title (buf, buf_offset, FIELD_TPS, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_QPS, NULL);
  if (full_info_flag == false)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_NUM_OF_SELECT_QUERIES, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUM_OF_INSERT_QUERIES, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUM_OF_UPDATE_QUERIES, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUM_OF_DELETE_QUERIES, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUM_OF_OTHERS_QUERIES, NULL);
    }

  buf_offset = print_title (buf, buf_offset, FIELD_LONG_TRANSACTION, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_LONG_QUERY, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_ERROR_QUERIES, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_UNIQUE_ERROR_QUERIES, NULL);
  if (full_info_flag && mnt_type == MONITOR_T_BROKER)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_CANCELED, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_ACCESS_MODE, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_SQL_LOG, NULL);
    }

  if (mnt_type == MONITOR_T_BROKER)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION_REJECTED, NULL);
    }
  else if (mnt_type == MONITOR_T_SHARDDB)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_REQUEST, NULL);
    }
  else if (mnt_type == MONITOR_T_PROXY)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION_REJECTED, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_RESTART, NULL);

      if (full_info_flag)
	{
	  buf_offset = print_title (buf, buf_offset, FIELD_STMT_POOL_RATIO, NULL);
	}
    }

  str_out ("%s", buf);
  print_newline ();

  for (i = 0; i < buf_offset; i++)
    {
      str_out ("%s", "=");
    }
  print_newline ();

  return;
}

static void
set_monitor_items (BR_MONITORING_ITEM * mnt_items, T_BROKER_INFO * br_info_p, T_SHM_APPL_SERVER * shm_appl,
		   T_SHM_PROXY * shm_proxy, MONITOR_TYPE mnt_type)
{
  int i, j;
  BR_MONITORING_ITEM *mnt_item_p = NULL;
  T_APPL_SERVER_INFO *as_info_p = NULL;
  T_PROXY_INFO *proxy_info_p = NULL;

  assert (mnt_type <= MONITOR_T_LAST);

  mnt_item_p = mnt_items;

  mnt_item_p->num_appl_server = br_info_p->appl_server_num;
  for (i = 0; i < br_info_p->appl_server_max_num; i++)
    {
      as_info_p = &(shm_appl->as_info[i]);

      mnt_item_p->num_request += as_info_p->num_request;
      mnt_item_p->num_connect += as_info_p->num_connect_requests;
      mnt_item_p->num_connect_reject += as_info_p->num_connect_rejected;
      if (full_info_flag && as_info_p->service_flag == ON)
	{
	  time_t cur_time = time (NULL);
	  bool time_expired = (cur_time - as_info_p->last_access_time >= state_interval);

	  if (as_info_p->uts_status == UTS_STATUS_BUSY && as_info_p->con_status != CON_STATUS_OUT_TRAN)
	    {
	      if (as_info_p->log_msg[0] == '\0')
		{
		  mnt_item_p->num_client_wait++;
		  if (time_expired)
		    {
		      mnt_item_p->num_client_wait_nsec++;
		    }
		}
	      else
		{
		  mnt_item_p->num_busy++;
		  if (time_expired)
		    {
		      mnt_item_p->num_busy_nsec++;
		    }
		}
	    }
#if defined(WINDOWS)
	  else if (as_info_p->uts_status == UTS_STATUS_BUSY_WAIT)
	    {
	      mnt_item_p->num_busy++;
	      if (time_expired)
		{
		  mnt_item_p->num_busy_nsec++;
		}
	    }
#endif
	}
      mnt_item_p->num_request += as_info_p->num_requests_received;
      mnt_item_p->num_tx += as_info_p->num_transactions_processed;
      mnt_item_p->num_qx += as_info_p->num_queries_processed;
      mnt_item_p->num_lt += as_info_p->num_long_transactions;
      mnt_item_p->num_lq += as_info_p->num_long_queries;
      mnt_item_p->num_eq += as_info_p->num_error_queries;
      mnt_item_p->num_eq_ui += as_info_p->num_unique_error_queries;
      mnt_item_p->num_interrupts += as_info_p->num_interrupts;
      mnt_item_p->num_select_query += as_info_p->num_select_queries;
      mnt_item_p->num_insert_query += as_info_p->num_insert_queries;
      mnt_item_p->num_update_query += as_info_p->num_update_queries;
      mnt_item_p->num_delete_query += as_info_p->num_delete_queries;
      mnt_item_p->num_others_query =
	(mnt_item_p->num_qx - mnt_item_p->num_select_query - mnt_item_p->num_insert_query -
	 mnt_item_p->num_update_query - mnt_item_p->num_delete_query);
    }

  return;
}

static void
print_monitor_items (BR_MONITORING_ITEM * mnt_items_cur, BR_MONITORING_ITEM * mnt_items_old, int num_mnt_items,
		     double elapsed_time, T_BROKER_INFO * br_info_p, T_SHM_APPL_SERVER * shm_appl,
		     MONITOR_TYPE mnt_type)
{
  int i;
  BR_MONITORING_ITEM *mnt_item_cur_p = NULL;
  BR_MONITORING_ITEM *mnt_item_old_p = NULL;
  BR_MONITORING_ITEM mnt_item;
  char buf[256];
#ifdef GET_PSINFO
  T_PSINFO proc_info;
  char time_str[32];
#endif

  assert (mnt_type <= MONITOR_T_LAST);

  for (i = 0; i < num_mnt_items; i++)
    {
      mnt_item_cur_p = &mnt_items_cur[i];
      mnt_item_old_p = &mnt_items_old[i];

      if (elapsed_time > 0)
	{
	  mnt_item.tps = (mnt_item_cur_p->num_tx - mnt_item_old_p->num_tx) / elapsed_time;
	  mnt_item.qps = (mnt_item_cur_p->num_qx - mnt_item_old_p->num_qx) / elapsed_time;
	  mnt_item.lts = mnt_item_cur_p->num_lt - mnt_item_old_p->num_lt;
	  mnt_item.lqs = mnt_item_cur_p->num_lq - mnt_item_old_p->num_lq;
	  mnt_item.eqs = mnt_item_cur_p->num_eq - mnt_item_old_p->num_eq;
	  mnt_item.eqs_ui = mnt_item_cur_p->num_eq_ui - mnt_item_old_p->num_eq_ui;
	  mnt_item.its = mnt_item_cur_p->num_interrupts - mnt_item_old_p->num_interrupt;
	  mnt_item.num_select_query = mnt_item_cur_p->num_select_query - mnt_item_old_p->num_select_query;
	  mnt_item.num_insert_query = mnt_item_cur_p->num_insert_query - mnt_item_old_p->num_insert_query;
	  mnt_item.num_update_query = mnt_item_cur_p->num_update_query - mnt_item_old_p->num_update_query;
	  mnt_item.num_delete_query = mnt_item_cur_p->num_delete_query - mnt_item_old_p->num_delete_query;
	  mnt_item.num_others_query = mnt_item_cur_p->num_others_query - mnt_item_old_p->num_others_query;

	  if (mnt_type == MONITOR_T_PROXY)
	    {
	      mnt_item.num_request_stmt =
		(mnt_item_cur_p->num_request_stmt - mnt_item_old_p->num_request_stmt) / elapsed_time;
	      mnt_item.num_request_stmt_in_pool =
		(mnt_item_cur_p->num_request_stmt_in_pool - mnt_item_old_p->num_request_stmt_in_pool) / elapsed_time;
	    }

	  mnt_item_cur_p->tps = mnt_item.tps;
	  mnt_item_cur_p->qps = mnt_item.qps;
	  mnt_item_cur_p->lts = mnt_item.lts;
	  mnt_item_cur_p->lqs = mnt_item.lqs;
	  mnt_item_cur_p->eqs = mnt_item.eqs;
	  mnt_item_cur_p->eqs_ui = mnt_item.eqs_ui;
	  mnt_item_cur_p->its = mnt_item.its;
	}
      else
	{
	  memcpy (&mnt_item, mnt_item_old_p, sizeof (mnt_item));
	}

      if (mnt_type == MONITOR_T_BROKER)
	{
	  print_value (FIELD_PID, &(br_info_p->pid), FIELD_T_INT);
	  if (full_info_flag)
	    {
#if defined(WINDOWS)
	      if (shm_appl->use_pdh_flag == TRUE)
		{
		  print_value (FIELD_PSIZE, &(br_info_p->pdh_workset), FIELD_T_INT);
		}
#else
	      int process_size;

	      process_size = getsize (br_info_p->pid);
	      print_value (FIELD_PSIZE, &process_size, FIELD_T_INT);
#endif
	    }
	  print_value (FIELD_PORT, &(br_info_p->port), FIELD_T_INT);
	}
      else if (mnt_type == MONITOR_T_SHARDDB)
	{
	  print_value (FIELD_SHARD_ID, &i, FIELD_T_INT);
	}
      else if (mnt_type == MONITOR_T_PROXY)
	{
	  int proxy_id = i + 1;
	  print_value (FIELD_PROXY_ID, &proxy_id, FIELD_T_INT);
	}

      print_value (FIELD_APPL_SERVER_NUM_TOTAL, &mnt_item_cur_p->num_appl_server, FIELD_T_INT);

      if (full_info_flag)
	{

	  print_value (FIELD_APPL_SERVER_NUM_CLIENT_WAIT, &mnt_item_cur_p->num_client_wait, FIELD_T_INT);
	  print_value (FIELD_APPL_SERVER_NUM_BUSY, &mnt_item_cur_p->num_busy, FIELD_T_INT);
	  print_value (FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC, &mnt_item_cur_p->num_client_wait_nsec, FIELD_T_INT);
	  print_value (FIELD_APPL_SERVER_NUM_BUSY_IN_SEC, &mnt_item_cur_p->num_busy_nsec, FIELD_T_INT);
	}

      if (mnt_type == MONITOR_T_BROKER)
	{
	  print_value (FIELD_JOB_QUEUE_ID, &(shm_appl->job_queue[0].id), FIELD_T_INT);
	}

#ifdef GET_PSINFO
      get_psinfo (br_info_p->pid, &proc_info);

      print_value (FIELD_THREAD, &(proc_info.num_thr), FIELD_T_INT);

      get_cpu_usage_string (buf, proc_info.pcpu);
      print_value (FIELD_CPU_USAGE, buf, FIELD_T_STRING);

      time_format (proc_info.cpu_time, time_str);
      print_value (FIELD_CPU_TIME, &time_str, FIELD_T_STRING);
#endif

      print_value (FIELD_TPS, &mnt_item.tps, FIELD_T_UINT64);
      print_value (FIELD_QPS, &mnt_item.qps, FIELD_T_UINT64);

      if (full_info_flag == false)
	{
	  print_value (FIELD_NUM_OF_SELECT_QUERIES, &mnt_item.num_select_query, FIELD_T_UINT64);
	  print_value (FIELD_NUM_OF_INSERT_QUERIES, &mnt_item.num_insert_query, FIELD_T_UINT64);
	  print_value (FIELD_NUM_OF_UPDATE_QUERIES, &mnt_item.num_update_query, FIELD_T_UINT64);
	  print_value (FIELD_NUM_OF_DELETE_QUERIES, &mnt_item.num_delete_query, FIELD_T_UINT64);
	  print_value (FIELD_NUM_OF_OTHERS_QUERIES, &mnt_item.num_others_query, FIELD_T_UINT64);
	}
      sprintf (buf, "%lu/%-.1f", mnt_item.lts, (shm_appl->long_transaction_time / 1000.0));
      print_value (FIELD_LONG_TRANSACTION, buf, FIELD_T_STRING);
      sprintf (buf, "%lu/%-.1f", mnt_item.lqs, (shm_appl->long_query_time / 1000.0));
      print_value (FIELD_LONG_QUERY, buf, FIELD_T_STRING);
      print_value (FIELD_ERROR_QUERIES, &mnt_item.eqs, FIELD_T_UINT64);
      print_value (FIELD_UNIQUE_ERROR_QUERIES, &mnt_item.eqs_ui, FIELD_T_UINT64);

      if (full_info_flag && mnt_type == MONITOR_T_BROKER)
	{
	  print_value (FIELD_CANCELED, &mnt_item.its, FIELD_T_INT64);
	  print_value (FIELD_ACCESS_MODE,
		       get_access_mode_string ((T_ACCESS_MODE_VALUE) br_info_p->access_mode,
					       br_info_p->replica_only_flag), FIELD_T_STRING);
	  print_value (FIELD_SQL_LOG, get_sql_log_mode_string ((T_SQL_LOG_MODE_VALUE) br_info_p->sql_log_mode),
		       FIELD_T_STRING);
	}

      if (mnt_type == MONITOR_T_BROKER)
	{
	  print_value (FIELD_NUMBER_OF_CONNECTION, &mnt_item_cur_p->num_connect, FIELD_T_UINT64);
	  print_value (FIELD_NUMBER_OF_CONNECTION_REJECTED, &mnt_item_cur_p->num_connect_reject, FIELD_T_UINT64);
	}
      else if (mnt_type == MONITOR_T_SHARDDB)
	{
	  print_value (FIELD_REQUEST, &mnt_item_cur_p->num_request, FIELD_T_UINT64);
	}
      else if (mnt_type == MONITOR_T_PROXY)
	{
	  print_value (FIELD_NUMBER_OF_CONNECTION, &mnt_item_cur_p->num_connect, FIELD_T_UINT64);
	  print_value (FIELD_NUMBER_OF_CONNECTION_REJECTED, &mnt_item_cur_p->num_connect_reject, FIELD_T_UINT64);
	  print_value (FIELD_RESTART, &mnt_item_cur_p->num_restart, FIELD_T_UINT64);

	  if (full_info_flag)
	    {
	      float stmt_pool_ratio;
	      if (mnt_item.num_request_stmt <= 0 || mnt_item.num_request_stmt_in_pool > mnt_item.num_request_stmt)
		{
		  print_value (FIELD_STMT_POOL_RATIO, (char *) "-", FIELD_T_STRING);
		}
	      else
		{
		  stmt_pool_ratio = (mnt_item.num_request_stmt_in_pool * 100) / mnt_item.num_request_stmt;
		  print_value (FIELD_STMT_POOL_RATIO, &stmt_pool_ratio, FIELD_T_FLOAT);
		}
	    }
	}

      print_newline ();
    }

  return;
}

static int
brief_monitor (char *br_vector, MONITOR_TYPE mnt_type, double elapsed_time)
{
  T_BROKER_INFO *br_info_p = NULL;
  T_SHM_APPL_SERVER *shm_appl = NULL;
  T_SHM_PROXY *shm_proxy_p = NULL;
  static BR_MONITORING_ITEM **mnt_items_old[MONITOR_T_LAST + 1] = { NULL, };
  static BR_MONITORING_ITEM *mnt_items_cur_p = NULL;
  BR_MONITORING_ITEM *mnt_items_old_p = NULL;
  int br_index;
  int max_num_mnt_items = 0;
  int num_mnt_items;
  int max_broker_name_size = FIELD_WIDTH_BROKER_NAME;
  int broker_name_size;

  assert (mnt_type <= MONITOR_T_LAST);

  for (br_index = 0; br_index < shm_br->num_broker; br_index++)
    {
      if (br_vector[br_index] == 0)
	{
	  continue;
	}

      broker_name_size = strlen (shm_br->br_info[br_index].name);
      if (broker_name_size > max_broker_name_size)
	{
	  max_broker_name_size = broker_name_size;
	}
    }
  if (max_broker_name_size > BROKER_NAME_LEN)
    {
      max_broker_name_size = BROKER_NAME_LEN;
    }
  fields[FIELD_BROKER_NAME].width = max_broker_name_size;

  if (mnt_type == MONITOR_T_BROKER)
    {
      print_monitor_header (mnt_type);
    }

  if (mnt_items_old[mnt_type] == NULL)
    {
      mnt_items_old[mnt_type] = (BR_MONITORING_ITEM **) calloc (sizeof (BR_MONITORING_ITEM *), shm_br->num_broker);
      if (mnt_items_old[mnt_type] == NULL)
	{
	  return -1;
	}
    }

  if (mnt_items_cur_p == NULL)
    {
      max_num_mnt_items = MAX (MAX (1, MAX_PROXY_NUM), MAX_SHARD_CONN);

      mnt_items_cur_p = (BR_MONITORING_ITEM *) malloc (sizeof (BR_MONITORING_ITEM) * max_num_mnt_items);
      if (mnt_items_cur_p == NULL)
	{
	  str_out ("%s", "malloc error");
	  print_newline ();
	  return -1;
	}
    }

  for (br_index = 0; br_index < shm_br->num_broker; br_index++)
    {
      br_info_p = &shm_br->br_info[br_index];

      if (br_vector[br_index] == 0)
	{
	  continue;
	}

      if (service_filter_value != SERVICE_UNKNOWN && service_filter_value != br_info_p->service_flag)
	{
	  continue;
	}

      if (mnt_type == MONITOR_T_BROKER)
	{
	  char broker_name[BROKER_NAME_LEN + 1];

	  snprintf (broker_name, BROKER_NAME_LEN, "%s", br_info_p->name);
	  broker_name[BROKER_NAME_LEN] = '\0';
	  str_out ("*%c", FIELD_DELIMITER);
	  print_value (FIELD_BROKER_NAME, broker_name, FIELD_T_STRING);
	}
      else if (mnt_type == MONITOR_T_SHARDDB || mnt_type == MONITOR_T_PROXY)
	{
	  str_out ("%% %s ", br_info_p->name);
	}

      if (br_info_p->service_flag != SERVICE_ON)
	{
	  str_out ("%s", "OFF");
	  print_newline ();
	  continue;
	}

      shm_appl = (T_SHM_APPL_SERVER *) uw_shm_open (br_info_p->appl_server_shm_id, SHM_APPL_SERVER, SHM_MODE_MONITOR);
      if (shm_appl == NULL)
	{
	  str_out ("%s", "shared memory open error");
	  print_newline ();
	  return -1;
	}

      num_mnt_items = get_num_monitor_items (mnt_type, shm_proxy_p);

      assert (num_mnt_items > 0);

      memset (mnt_items_cur_p, 0, sizeof (BR_MONITORING_ITEM) * num_mnt_items);

      if (mnt_items_old[mnt_type][br_index] == NULL)
	{
	  mnt_items_old[mnt_type][br_index] =
	    (BR_MONITORING_ITEM *) calloc (sizeof (BR_MONITORING_ITEM), num_mnt_items);
	  if (mnt_items_old[mnt_type][br_index] == NULL)
	    {
	      str_out ("%s", "malloc error");
	      print_newline ();
	      goto error;
	    }
	}

      mnt_items_old_p = mnt_items_old[mnt_type][br_index];

      set_monitor_items (mnt_items_cur_p, br_info_p, shm_appl, shm_proxy_p, mnt_type);

      print_monitor_items (mnt_items_cur_p, mnt_items_old_p, num_mnt_items, elapsed_time, br_info_p, shm_appl,
			   mnt_type);
      memcpy (mnt_items_old_p, mnt_items_cur_p, sizeof (BR_MONITORING_ITEM) * num_mnt_items);

      if (shm_appl)
	{
	  uw_shm_detach (shm_appl);
	  shm_appl = NULL;
	}

      if (shm_proxy_p)
	{
	  uw_shm_detach (shm_proxy_p);
	  shm_proxy_p = NULL;
	}
    }

  return 0;

error:
  if (shm_appl)
    {
      uw_shm_detach (shm_appl);
    }
  if (shm_proxy_p)
    {
      uw_shm_detach (shm_proxy_p);
    }

  return -1;
}

#ifdef GET_PSINFO
static void
time_format (int t, char *time_str)
{
  int min, sec;

  min = t / 60;
  sec = t % 60;
  sprintf (time_str, "%d:%02d", min, sec);
}
#endif

static void
print_appl_header (bool use_pdh_flag)
{
  char buf[256];
  char line_buf[256];
  int buf_offset = 0;
  int i;

  buf_offset = print_title (buf, buf_offset, FIELD_ID, NULL);

  buf_offset = print_title (buf, buf_offset, FIELD_PID, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_QPS, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_LQS, NULL);
#if defined(WINDOWS)
  buf_offset = print_title (buf, buf_offset, FIELD_PORT, NULL);
#endif
#if defined(WINDOWS)
  if (use_pdh_flag == true)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_PSIZE, NULL);
    }
#else
  buf_offset = print_title (buf, buf_offset, FIELD_PSIZE, NULL);
#endif
  buf_offset = print_title (buf, buf_offset, FIELD_STATUS, NULL);
#if 0
  buf_offset = print_title (buf, buf_offset, FIELD_PORT, NULL);
#endif
#ifdef GET_PSINFO
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_USAGE, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_TIME, NULL);
#elif WINDOWS
  if (use_pdh_flag == true)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_CPU_USAGE, NULL);
    }
#endif
  if (full_info_flag)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_LAST_ACCESS_TIME, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_DB_NAME, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_HOST, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_LAST_CONNECT_TIME, NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_CLIENT_IP, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_CLIENT_VERSION, NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_SQL_LOG_MODE, NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_TRANSACTION_STIME, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_CONNECT, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_RESTART, NULL);

    }

  for (i = 0; i < buf_offset; i++)
    line_buf[i] = '-';
  line_buf[i] = '\0';

  str_out ("%s", line_buf);
  print_newline ();
  str_out ("%s", buf);
  print_newline ();
  str_out ("%s", line_buf);
  print_newline ();
}

#if defined(WINDOWS)
static void
refresh ()
{
}

static void
move (int x, int y)
{
  COORD pos;
  pos.X = x;
  pos.Y = y;

  SetConsoleCursorPosition (h_console, pos);
}

static void
clear ()
{
  COORD pos = {
    0, 0
  };
  DWORD size;
  FillConsoleOutputCharacter (h_console, ' ', scr_info.dwSize.X * scr_info.dwSize.Y, pos, &size);
}

static void
clrtobot ()
{
  CONSOLE_SCREEN_BUFFER_INFO scr_buf_info;
  DWORD size;

  if (!GetConsoleScreenBufferInfo (h_console, &scr_buf_info))
    {
      return;
    }
  FillConsoleOutputCharacter (h_console, ' ',
			      (scr_buf_info.dwSize.Y - scr_buf_info.dwCursorPosition.Y) * scr_buf_info.dwSize.X,
			      scr_buf_info.dwCursorPosition, &size);
}

static void
clrtoeol ()
{
  CONSOLE_SCREEN_BUFFER_INFO scr_buf_info;
  DWORD size;

  if (!GetConsoleScreenBufferInfo (h_console, &scr_buf_info))
    {
      return;
    }
  FillConsoleOutputCharacter (h_console, ' ', scr_buf_info.dwSize.X - scr_buf_info.dwCursorPosition.X + 1,
			      scr_buf_info.dwCursorPosition, &size);
  move (scr_buf_info.dwSize.X, scr_buf_info.dwCursorPosition.Y);
}

static void
endwin ()
{
  clear ();
  move (0, 0);
}
#else
static void
refresh ()
{
  fflush (stdout);
}

static void
move (int x, int y)
{
  int lines = -1;
  int cols = -1;

  if (tgetent (NULL, term) == 1)
    {
      lines = tgetnum ("li");
      cols = tgetnum ("co");
    }

  tty_Lines = lines < 0 ? tty_Lines : lines;
  tty_Cols = cols < 0 ? tty_Cols : cols;

  currentY = y;
  tputs (tgoto (cm, x, y), 1, putchar);
  fflush (stdout);
}

static void
clrtobot ()
{
  tputs (cd, 1, putchar);
  fflush (stdout);
}

static void
clrtoeol ()
{
  tputs (ce, 1, putchar);
  fflush (stdout);
}

static void
endwin ()
{
  tcsetattr (0, TCSAFLUSH, &oterm);
  clear ();
  move (0, 0);

  if (dl_handle != NULL)
    {
      dlclose (dl_handle);
      dl_handle = NULL;
    }
}

static void
addstr (const char *str)
{
  /* line# starts at 0 */
  if (str == NULL || currentY >= tty_Lines)
    {
      return;
    }

  currentY += num_newlines (str);

  if (currentY >= tty_Lines)
    {
      return;
    }

  fprintf (stdout, "%s", str);
  fflush (stdout);
}

static int
getch ()
{
  fd_set readfds;
  struct timeval tv, *tv_p = &tv;
  int fd = fileno (stdin);
  int timeout = get_timeout ();
  int ret = -1;

  if (timeout < 0)
    {
      tv_p = NULL;
    }
  else
    {
      tv.tv_sec = timeout / 1000;
      tv.tv_usec = timeout % 1000;
    }

  FD_ZERO (&readfds);
  FD_SET (fd, &readfds);

  ret = select (fd + 1, &readfds, (fd_set *) 0, (fd_set *) 0, tv_p);

  if (ret <= 0)
    {
      return -1;
    }

  return fgetc (stdin);
}

static void
clear ()
{
  currentY = 0;
  tputs (cl, 1, putchar);
  fflush (stdout);
}

static int
num_newlines (const char *str)
{
  int num_nl = 0;
  int i = 0;

  if (str == NULL)
    {
      return 0;
    }


  while (str[i])
    {
      if (str[i++] == '\n')
	{
	  num_nl++;
	  num_Chars = 0;
	}
      else
	{
	  num_Chars++;
	}

      if (num_Chars == tty_Cols)
	{
	  num_nl++;
	  num_Chars = 0;
	}
    }

  return num_nl;
}

static int
tty_noblock ()
{
  struct termios term;

  if (tcgetattr (fileno (stdin), &term) < 0)
    {
      return -1;
    }

  oterm = term;

  term.c_iflag &= ~(BRKINT | INPCK | ISTRIP | IXON);
  term.c_cflag |= (CS8);
  term.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);

  term.c_cc[VMIN] = 0;
  term.c_cc[VTIME] = 8;		/* after a byte or .8 seconds */

  /* put terminal in raw mode after flushing */
  if (tcsetattr (0, TCSAFLUSH, &term) < 0)
    {
      return -2;
    }

  return 0;
}

static int
get_timeout ()
{
  return Stdin_timer;
}

static void
timeout (int delay)
{
  Stdin_timer = delay < 0 ? 0 : delay;
}

static void
noecho ()
{
}

static void *
initscr ()
{
  char tinfo_so[PATH_MAX];
  int major_version;
  int ret = -1;

  for (major_version = TINFO_HIGH_VERSION; major_version >= TINFO_LOW_VERSION; major_version--)
    {
      sprintf (tinfo_so, "libtinfo.so.%d", major_version);
      if ((dl_handle = dlopen (tinfo_so, RTLD_LAZY | RTLD_GLOBAL)) != NULL)
	{
	  break;
	}
    }

  if (dl_handle == NULL)
    {
      fprintf (stderr, "ERROR: Cannot load tinfo library. Please install tinfo library.\n");
      return NULL;
    }

  if ((tgoto = (tgoto_func_t) dlsym (dl_handle, "tgoto")) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'tgoto' function in %s.\n", tinfo_so);
      return NULL;
    }

  if ((tgetent = (tgetent_func_t) dlsym (dl_handle, "tgetent")) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'tgetent' function in %s.\n", tinfo_so);
      return NULL;
    }

  if ((tgetstr = (tgetstr_func_t) dlsym (dl_handle, "tgetstr")) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'tgetstr' function in %s.\n", tinfo_so);
      return NULL;
    }

  if ((tputs = (tputs_func_t) dlsym (dl_handle, "tputs")) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'tputs' function in %s.\n", tinfo_so);
      return NULL;
    }

  tgetnum = (tgetnum_func_t) dlsym (dl_handle, "tgetnum");

  term = getenv ("TERM");
  if (term == NULL)
    {
      term = "xterm";
    }

  if (tgetent (NULL, term) != 1)
    {
      fprintf (stderr, "ERROR: Cannot find TERM type.");
      return NULL;
    }

  if ((cm = tgetstr ("cm", NULL)) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'cursor motion (cm)' capability for the terminal\n");
      return NULL;
    }

  if ((cd = tgetstr ("cd", NULL)) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'clear to end of screen (cd)' capability for the terminal\n");
      return NULL;
    }

  if ((ce = tgetstr ("ce", NULL)) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'Clear to end of line (ce)' capability for the terminal\n");
      return NULL;
    }

  if ((cl = tgetstr ("cl", NULL)) == NULL)
    {
      fprintf (stderr, "ERROR: Cannot find 'Clear screen and cursor home (cl)' capability for the terminal\n");
      return NULL;
    }

  tty_Lines = tgetnum ("li");
  if (tty_Lines < 1)
    {
      fprintf (stderr, "ERROR: cannot get #LINES for the terminal, check TERM environment variable\n");
      return NULL;
    }

  tty_Cols = tgetnum ("co");
  if (tty_Cols < 1)
    {
      fprintf (stderr, "ERROR: cannot get #COLUMNS for the terminal, check TERM environment variable\n");
      return NULL;
    }

  if ((ret = tty_noblock ()) < 0)
    {
      fprintf (stderr, "ERROR: Cannot set terminal: error = %d", ret);
      return NULL;
    }

  clear ();

  return dl_handle;
}
#endif

static int
print_title (char *buf_p, int buf_offset, FIELD_NAME name, const char *new_title_p)
{
  struct status_field *field_p = NULL;
  const char *title_p = NULL;

  assert (buf_p != NULL);
  assert (buf_offset >= 0);

  field_p = &fields[name];

  if (new_title_p != NULL)
    {
      title_p = new_title_p;
    }
  else
    {
      title_p = field_p->title;
    }

  switch (field_p->name)
    {
    case FIELD_BROKER_NAME:
      buf_offset += sprintf (buf_p + buf_offset, "%c%c", FIELD_DELIMITER, FIELD_DELIMITER);
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%-*s", field_p->width, title_p);
	}
      else
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%*s", field_p->width, title_p);
	}
      break;
    default:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%-*s", field_p->width, title_p);
	}
      else
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%*s", field_p->width, title_p);
	}
      break;
    }
  buf_offset += sprintf (buf_p + buf_offset, "%c", FIELD_DELIMITER);

  return buf_offset;
}

static void
print_value (FIELD_NAME name, const void *value_p, FIELD_TYPE type)
{
  struct status_field *field_p = NULL;
  struct tm cur_tm;
  char time_buf[64];

  assert (value_p != NULL);

  field_p = &fields[name];

  switch (type)
    {
    case FIELD_T_INT:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*d", field_p->width, *(const int *) value_p);
	}
      else
	{
	  str_out ("%*d", field_p->width, *(const int *) value_p);
	}
      break;
    case FIELD_T_STRING:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*s", field_p->width, (const char *) value_p);
	}
      else
	{
	  str_out ("%*s", field_p->width, (const char *) value_p);
	}
      break;
    case FIELD_T_FLOAT:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*f", field_p->width, *(const float *) value_p);
	}
      else
	{
	  str_out ("%*f", field_p->width, *(const float *) value_p);
	}
      break;
    case FIELD_T_UINT64:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*lu", field_p->width, *(const UINT64 *) value_p);
	}
      else
	{
	  str_out ("%*lu", field_p->width, *(const UINT64 *) value_p);
	}
      break;
    case FIELD_T_INT64:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*ld", field_p->width, *(const INT64 *) value_p);
	}
      else
	{
	  str_out ("%*ld", field_p->width, *(const INT64 *) value_p);
	}
      break;
    case FIELD_T_TIME:
      localtime_r ((const time_t *) value_p, &cur_tm);
      cur_tm.tm_year += 1900;
      sprintf (time_buf, "%02d/%02d/%02d %02d:%02d:%02d", cur_tm.tm_year, cur_tm.tm_mon + 1, cur_tm.tm_mday,
	       cur_tm.tm_hour, cur_tm.tm_min, cur_tm.tm_sec);
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*s", field_p->width, time_buf);
	}
      else
	{
	  str_out ("%*s", field_p->width, time_buf);
	}
    default:
      break;
    }
  str_out ("%c", FIELD_DELIMITER);
}

static int
get_num_monitor_items (MONITOR_TYPE mnt_type, T_SHM_PROXY * shm_proxy_p)
{
  int num_mnt_items;

  if (mnt_type == MONITOR_T_BROKER)
    {
      num_mnt_items = 1;
    }
  else if (mnt_type == MONITOR_T_SHARDDB)
    {
      num_mnt_items = shm_proxy_p->proxy_info[0].max_shard;
    }
  else if (mnt_type == MONITOR_T_PROXY)
    {
      num_mnt_items = shm_proxy_p->num_proxy;
    }
  else
    {
      /* invalid MONITOR_TYPE */
      assert (false);
      num_mnt_items = 0;
    }

  return num_mnt_items;
}

static const char *
get_sql_log_mode_string (T_SQL_LOG_MODE_VALUE mode)
{
  switch (mode)
    {
    case SQL_LOG_MODE_NONE:
      return "NONE";
    case SQL_LOG_MODE_ERROR:
      return "ERROR";
    case SQL_LOG_MODE_TIMEOUT:
      return "TIMEOUT";
    case SQL_LOG_MODE_NOTICE:
      return "NOTICE";
    case SQL_LOG_MODE_ALL:
      return "ALL";
    default:
      return "-";
    }
}

static const char *
get_access_mode_string (T_ACCESS_MODE_VALUE mode, int replica_only_flag)
{
  switch (mode)
    {
    case READ_ONLY_ACCESS_MODE:
      return (replica_only_flag ? "RO-REPLICA" : "RO");
    case SLAVE_ONLY_ACCESS_MODE:
      return (replica_only_flag ? "SO-REPLICA" : "SO");
    case READ_WRITE_ACCESS_MODE:
      return (replica_only_flag ? "RW-REPLICA" : "RW");
    default:
      return "--";
    }
}

static const char *
get_status_string (T_APPL_SERVER_INFO * as_info_p, char appl_server)
{
  assert (as_info_p != NULL);

  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      if (IS_APPL_SERVER_TYPE_CAS (appl_server))
	{
	  if (as_info_p->con_status == CON_STATUS_OUT_TRAN)
	    {
	      return "CLOSE_WAIT";
	    }
	  else if (as_info_p->log_msg[0] == '\0')
	    {
	      return "CLIENT_WAIT";
	    }
	  else
	    {
	      return "BUSY";
	    }
	}
      else
	{
	  return "BUSY";
	}
    }
#if defined(WINDOWS)
  else if (as_info_p->uts_status == UTS_STATUS_BUSY_WAIT)
    {
      return "BUSY";
    }
#endif
  else if (as_info_p->uts_status == UTS_STATUS_RESTART)
    {
      return "INITIALIZE";
    }
  else if (as_info_p->uts_status == UTS_STATUS_CON_WAIT)
    {
      return "CON WAIT";
    }
  else
    {
      return "IDLE";
    }

  return NULL;
}

static void
get_cpu_usage_string (char *buf_p, float usage)
{
  assert (buf_p != NULL);

  if (usage >= 0)
    {
      sprintf (buf_p, "%.2f", usage);
    }
  else
    {
      sprintf (buf_p, " - ");
    }
}
