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
 * broker.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#if !defined(WINDOWS)
#include <pthread.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <sys/un.h>
#else
#include  <io.h>
#endif

#ifdef BROKER_DEBUG
#include <sys/time.h>
#endif

#include <poll.h>

#include "util_func.h"

#include "cas_error.h"
#include "cas_common.h"
#include "broker_error.h"
#include "broker_env_def.h"
#include "broker_shm.h"
#include "broker_msg.h"
#include "broker_process_size.h"
#include "broker_util.h"
#include "broker_access_list.h"
#include "broker_filename.h"
#include "broker_er_html.h"
#include "broker_send_fd.h"
#include "error_manager.h"

#if defined(WINDOWS)
#include "broker_wsa_init.h"
#endif

#ifdef WIN_FW
#if !defined(WINDOWS)
#error DEFINE ERROR
#endif
#endif

#ifdef BROKER_RESTART_DEBUG
#define		PS_CHK_PERIOD		30
#else
#define		PS_CHK_PERIOD		600
#endif

#ifdef ASYNC_MODE
#ifdef HPUX10_2
#define		SELECT_MASK		int
#else
#define		SELECT_MASK		fd_set
#endif
#endif

#define		IP_ADDR_STR_LEN		20
#define		BUFFER_SIZE		ONE_K

#define		ENV_BUF_INIT_SIZE	512
#define		ALIGN_ENV_BUF_SIZE(X)	\
	((((X) + ENV_BUF_INIT_SIZE) / ENV_BUF_INIT_SIZE) * ENV_BUF_INIT_SIZE)

#define         MONITOR_SERVER_INTERVAL 5

#ifdef BROKER_DEBUG
#define		BROKER_LOG(X)			\
		do {				\
		  FILE		*fp;		\
		  fp = fopen("broker.log", "a");	\
		  if (fp) {			\
		    struct timeval tv;		\
		    gettimeofday(&tv, NULL);	\
		    fprintf(fp, "%d %d.%06d %s\n", __LINE__, (int)tv.tv_sec, (int)tv.tv_usec, X);	\
		    fclose(fp);			\
		  }				\
		} while (0)
#define		BROKER_LOG_INT(X)			\
		do {				\
		  FILE	*fp;			\
		  struct timeval tv;		\
		  gettimeofday(&tv, NULL);	\
		  fp = fopen("broker.log", "a");		\
		  if (fp) {			\
		    fprintf(fp, "%d %d.%06d %s=%d\n", __LINE__, (int)tv.tv_sec, (int)tv.tv_usec, #X, X);			\
		    fclose(fp);			\
		  }				\
		} while (0)
#endif

#ifdef SESSION_LOG
#define		SESSION_LOG_WRITE(IP, SID, APPL, INDEX)	\
		do {				\
		  FILE	*fp;			\
		  struct timeval tv;		\
		  char	ip_str[64];		\
		  gettimeofday(&tv, NULL);	\
		  ip2str(IP, ip_str);		\
		  fp = fopen("session.log", "a");	\
		  if (fp) {			\
		    fprintf(fp, "%d %-15s %d %d.%06d %s %d \n", br_index, ip_str, (int) SID, tv.tv_sec, tv.tv_usec, APPL, INDEX); \
		    fclose(fp);			\
		  }				\
		} while (0)
#endif

#ifdef _EDU_
#define		EDU_KEY	"86999522480552846466422480899195252860256028745"
#endif

#define V3_WRITE_HEADER_OK_FILE_SOCK(sock_fd)	\
	do {				\
	  char          buf[V3_RESPONSE_HEADER_SIZE];	\
	  memset(buf, '\0', sizeof(buf));	\
	  sprintf(buf, V3_HEADER_OK);	\
	  write_to_client(sock_fd, buf, sizeof(buf));	\
	} while(0);

#define V3_WRITE_HEADER_ERR_SOCK(sockfd)		\
	do {				\
	  char          buf[V3_RESPONSE_HEADER_SIZE];	\
	  memset(buf, '\0', sizeof(buf));	\
	  sprintf(buf, V3_HEADER_ERR);		\
	  write_to_client(sockfd, buf, sizeof(buf)); \
	} while(0);

#define SET_BROKER_ERR_CODE()			\
	do {					\
	  if (shm_br && br_index >= 0) {	\
	    shm_br->br_info[br_index].err_code = uw_get_error_code();	\
	    shm_br->br_info[br_index].os_err_code = uw_get_os_error_code(); \
	  }					\
	} while (0)

#define SET_BROKER_OK_CODE()				\
	do {						\
	  if (shm_br && br_index >= 0) {		\
	    shm_br->br_info[br_index].err_code = 0;	\
	  }						\
	} while (0)

#define CAS_SEND_ERROR_CODE(FD, VAL)	\
	do {				\
	  int   write_val;		\
	  write_val = htonl(VAL);	\
	  write_to_client(FD, (char*) &write_val, 4);	\
	} while (0)

#define JOB_COUNT_MAX		130000000

/* num of collecting counts per monitoring interval */
#define NUM_COLLECT_COUNT_PER_INTVL     4
#define HANG_COUNT_THRESHOLD_RATIO      0.5

#if defined(WINDOWS)
#define F_OK	0
#else
#define SOCKET_TIMEOUT_SEC	2
#endif

/* server state */
enum SERVER_STATE
{
  SERVER_STATE_UNKNOWN = 0,
  SERVER_STATE_DEAD = 1,
  SERVER_STATE_DEREGISTERED = 2,
  SERVER_STATE_STARTED = 3,
  SERVER_STATE_NOT_REGISTERED = 4,
  SERVER_STATE_REGISTERED = 5,
  SERVER_STATE_REGISTERED_AND_TO_BE_STANDBY = 6,
  SERVER_STATE_REGISTERED_AND_ACTIVE = 7,
  SERVER_STATE_REGISTERED_AND_TO_BE_ACTIVE = 8
};

typedef struct t_clt_table T_CLT_TABLE;
struct t_clt_table
{
  SOCKET clt_sock_fd;
  char ip_addr[IP_ADDR_STR_LEN];
};
static void cleanup (int signo);
static int init_env (void);
static int broker_init_shm (void);

static void cas_monitor_worker (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index, int *busy_uts);
static void psize_check_worker (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index);

static THREAD_FUNC receiver_thr_f (void *arg);
static THREAD_FUNC dispatch_thr_f (void *arg);
static THREAD_FUNC psize_check_thr_f (void *arg);
static THREAD_FUNC cas_monitor_thr_f (void *arg);
static THREAD_FUNC hang_check_thr_f (void *arg);
static THREAD_FUNC server_monitor_thr_f (void *arg);

static int read_nbytes_from_client (SOCKET sock_fd, char *buf, int size);

#if defined(WIN_FW)
static THREAD_FUNC service_thr_f (void *arg);
static int process_cas_request (int cas_pid, int as_index, SOCKET clt_sock_fd, SOCKET srv_sock_fd);
static int read_from_cas_client (SOCKET sock_fd, char *buf, int size, int as_index, int cas_pid);
#endif

static int write_to_client (SOCKET sock_fd, char *buf, int size);
static int write_to_client_with_timeout (SOCKET sock_fd, char *buf, int size, int timeout_sec);
static int read_from_client (SOCKET sock_fd, char *buf, int size);
static int read_from_client_with_timeout (SOCKET sock_fd, char *buf, int size, int timeout_sec);
static int read_buffer_async (SOCKET sock_fd, char *buf, int size, int timeout);
static int write_buffer_async (SOCKET sock_fd, char *buf, int size, int timeout);
static int run_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index);
static int stop_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index);
static void restart_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index);

static SOCKET connect_srv (char *br_name, int as_index);
static int find_idle_cas (void);
static int find_drop_as_index (void);
static int find_add_as_index (void);
static bool broker_add_new_cas (void);

static void check_cas_log (char *br_name, T_APPL_SERVER_INFO * as_info_p, int as_index);

static void get_as_sql_log_filename (char *log_filename, int len, char *broker_name, T_APPL_SERVER_INFO * as_info_p,
				     int as_index);
static void get_as_slow_log_filename (char *log_filename, int len, char *broker_name, T_APPL_SERVER_INFO * as_info_p,
				      int as_index);

static int insert_db_server_check_list (T_DB_SERVER * list_p, int check_list_cnt, const char *db_name,
					const char *db_host);

#if defined(WINDOWS)
static int get_cputime_sec (int pid);
#endif

static SOCKET sock_fd;
static struct sockaddr_in sock_addr;
static int sock_addr_len;

static T_SHM_BROKER *shm_br = NULL;
static T_SHM_APPL_SERVER *shm_appl;
static T_BROKER_INFO *br_info_p = NULL;

static int br_index = -1;

#if defined(WIN_FW)
static int num_thr;
#endif

static pthread_cond_t clt_table_cond;
static pthread_mutex_t clt_table_mutex;
static pthread_mutex_t run_appl_mutex;
static pthread_mutex_t broker_shm_mutex;

static char run_appl_server_flag = 0;

static int current_dropping_as_index = -1;

static int process_flag = 1;

static int num_busy_uts = 0;

static int max_open_fd = 128;

#if defined(WIN_FW)
static int last_job_fetch_time;
static time_t last_session_id = 0;
static T_MAX_HEAP_NODE *session_request_q;
#endif

static int hold_job = 0;

static bool
broker_add_new_cas (void)
{
  int cur_appl_server_num;
  int add_as_index;
  int pid;

  cur_appl_server_num = shm_br->br_info[br_index].appl_server_num;

  /* ADD UTS */
  if (cur_appl_server_num >= shm_br->br_info[br_index].appl_server_max_num)
    {
      return false;
    }

  add_as_index = find_add_as_index ();
  if (add_as_index < 0)
    {
      return false;
    }

  pid = run_appl_server (&(shm_appl->as_info[add_as_index]), br_index, add_as_index);
  if (pid <= 0)
    {
      return false;
    }

  pthread_mutex_lock (&broker_shm_mutex);
  shm_appl->as_info[add_as_index].pid = pid;
  shm_appl->as_info[add_as_index].psize = getsize (pid);
  shm_appl->as_info[add_as_index].psize_time = time (NULL);
  shm_appl->as_info[add_as_index].uts_status = UTS_STATUS_IDLE;
  shm_appl->as_info[add_as_index].service_flag = SERVICE_ON;
  shm_appl->as_info[add_as_index].reset_flag = FALSE;

  memset (&shm_appl->as_info[add_as_index].cas_clt_ip[0], 0x0, sizeof (shm_appl->as_info[add_as_index].cas_clt_ip));
  shm_appl->as_info[add_as_index].cas_clt_port = 0;
  shm_appl->as_info[add_as_index].driver_version[0] = '\0';

  (shm_br->br_info[br_index].appl_server_num)++;
  (shm_appl->num_appl_server)++;
  pthread_mutex_unlock (&broker_shm_mutex);

  return true;
}

static bool
broker_drop_one_cas_by_time_to_kill (void)
{
  int cur_appl_server_num, wait_job_cnt;
  int drop_as_index;
  T_APPL_SERVER_INFO *drop_as_info;

  /* DROP UTS */
  cur_appl_server_num = shm_br->br_info[br_index].appl_server_num;
  wait_job_cnt = shm_appl->job_queue[0].id + hold_job;
  wait_job_cnt -= (cur_appl_server_num - num_busy_uts);

  if (cur_appl_server_num <= shm_br->br_info[br_index].appl_server_min_num || wait_job_cnt > 0)
    {
      return false;
    }

  drop_as_index = find_drop_as_index ();
  if (drop_as_index < 0)
    {
      return false;
    }

  pthread_mutex_lock (&broker_shm_mutex);
  current_dropping_as_index = drop_as_index;
  drop_as_info = &shm_appl->as_info[drop_as_index];
  drop_as_info->service_flag = SERVICE_OFF_ACK;
  pthread_mutex_unlock (&broker_shm_mutex);

  CON_STATUS_LOCK (drop_as_info, CON_STATUS_LOCK_BROKER);
  if (drop_as_info->uts_status == UTS_STATUS_IDLE)
    {
      /* do nothing */
    }
  else if (drop_as_info->cur_keep_con == KEEP_CON_AUTO && drop_as_info->uts_status == UTS_STATUS_BUSY
	   && drop_as_info->con_status == CON_STATUS_OUT_TRAN
	   && time (NULL) - drop_as_info->last_access_time > shm_br->br_info[br_index].time_to_kill)
    {
      drop_as_info->con_status = CON_STATUS_CLOSE;
    }
  else
    {
      drop_as_info->service_flag = SERVICE_ON;
      drop_as_index = -1;
    }
  CON_STATUS_UNLOCK (drop_as_info, CON_STATUS_LOCK_BROKER);

  if (drop_as_index >= 0)
    {
      pthread_mutex_lock (&broker_shm_mutex);
      (shm_br->br_info[br_index].appl_server_num)--;
      (shm_appl->num_appl_server)--;
      pthread_mutex_unlock (&broker_shm_mutex);

      stop_appl_server (drop_as_info, br_index, drop_as_index);
    }
  current_dropping_as_index = -1;

  return true;
}

#if defined(WINDOWS)
int WINAPI
WinMain (HINSTANCE hInstance,	// handle to current instance
	 HINSTANCE hPrevInstance,	// handle to previous instance
	 LPSTR lpCmdLine,	// pointer to command line
	 int nShowCmd		// show state of window
  )
#else
int
main (int argc, char *argv[])
#endif
{
  pthread_t receiver_thread;
  pthread_t dispatch_thread;
  pthread_t cas_monitor_thread;
  pthread_t psize_check_thread;
  pthread_t hang_check_thread;
  pthread_t server_monitor_thread;
  pthread_t proxy_monitor_thread;
#if !defined(WINDOWS)
  pthread_t proxy_listener_thread;
#endif /* !WINDOWS */

#if defined(WIN_FW)
  pthread_t service_thread;
  int *thr_index;
  int i;
#endif
  int error;

  error = broker_init_shm ();
  if (error)
    {
      goto error1;
    }

  signal (SIGTERM, cleanup);
  signal (SIGINT, cleanup);
#if !defined(WINDOWS)
  signal (SIGCHLD, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);
#endif

  pthread_cond_init (&clt_table_cond, NULL);
  pthread_mutex_init (&clt_table_mutex, NULL);
  pthread_mutex_init (&run_appl_mutex, NULL);
  pthread_mutex_init (&broker_shm_mutex, NULL);

#if defined(WINDOWS)
  if (wsa_initialize () < 0)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, 0);
      goto error1;
    }
#endif /* WINDOWS */

  if (uw_acl_make (shm_br->br_info[br_index].acl_file) < 0)
    {
      goto error1;
    }

  if (init_env () == -1)
    {
      goto error1;
    }

  {
#if defined(WIN_FW)
    num_thr = shm_br->br_info[br_index].appl_server_max_num;

    thr_index = (int *) malloc (sizeof (int) * num_thr);
    if (thr_index == NULL)
      {
	UW_SET_ERROR_CODE (UW_ER_NO_MORE_MEMORY, 0);
	goto error1;
      }

    /* initialize session request queue. queue size is 1 */
    session_request_q = (T_MAX_HEAP_NODE *) malloc (sizeof (T_MAX_HEAP_NODE) * num_thr);
    if (session_request_q == NULL)
      {
	UW_SET_ERROR_CODE (UW_ER_NO_MORE_MEMORY, 0);
	goto error1;
      }
    for (i = 0; i < num_thr; i++)
      {
	session_request_q[i].clt_sock_fd = INVALID_SOCKET;
      }
#endif
  }

  set_cubrid_file (FID_SQL_LOG_DIR, shm_appl->log_dir);
  set_cubrid_file (FID_SLOW_LOG_DIR, shm_appl->slow_log_dir);

  while (shm_br->br_info[br_index].ready_to_service != true)
    {
      SLEEP_MILISEC (0, 200);
    }

  THREAD_BEGIN (receiver_thread, receiver_thr_f, NULL);

  {
    THREAD_BEGIN (dispatch_thread, dispatch_thr_f, NULL);
  }
  THREAD_BEGIN (psize_check_thread, psize_check_thr_f, NULL);
  THREAD_BEGIN (cas_monitor_thread, cas_monitor_thr_f, NULL);

  if (shm_br->br_info[br_index].monitor_hang_flag)
    {
      THREAD_BEGIN (hang_check_thread, hang_check_thr_f, NULL);
    }

  {
#if defined(WIN_FW)
    for (i = 0; i < num_thr; i++)
      {
	thr_index[i] = i;
	THREAD_BEGIN (service_thread, service_thr_f, thr_index + i);
	shm_appl->as_info[i].last_access_time = time (NULL);
	shm_appl->as_info[i].transaction_start_time = (time_t) 0;
	if (i < shm_br->br_info[br_index].appl_server_min_num)
	  {
	    shm_appl->as_info[i].service_flag = SERVICE_ON;
	  }
	else
	  {
	    shm_appl->as_info[i].service_flag = SERVICE_OFF_ACK;
	  }
      }
#endif

    SET_BROKER_OK_CODE ();

    while (process_flag)
      {
	SLEEP_MILISEC (0, 100);

	if (shm_br->br_info[br_index].auto_add_appl_server == OFF)
	  {
	    continue;
	  }

	broker_drop_one_cas_by_time_to_kill ();
      }				/* end of while (process_flag) */
  }				/* end of if (SHARD == OFF) */

error1:

  SET_BROKER_ERR_CODE ();
  return -1;
}

static void
cleanup (int signo)
{
  signal (signo, SIG_IGN);

  process_flag = 0;
#ifdef SOLARIS
  SLEEP_MILISEC (1, 0);
#endif
  CLOSE_SOCKET (sock_fd);
  exit (0);
}

static bool
di_understand_renewed_error_code (const char *driver_info)
{
#define IS_SET_BIT(C,B)	(((C) & (B)) == (B))
  if (!IS_SET_BIT (driver_info[SRV_CON_MSG_IDX_PROTO_VERSION], CAS_PROTO_INDICATOR))
    {
      return false;
    }

  return IS_SET_BIT (driver_info[DRIVER_INFO_FUNCTION_FLAG], BROKER_RENEWED_ERROR_CODE);
}

static void
send_error_to_driver (int sock, int error, char *driver_info)
{
  int write_val;
  int driver_version;

  driver_version = CAS_MAKE_PROTO_VER (driver_info);

  if (error == NO_ERROR)
    {
      write_val = 0;
    }
  else
    {
      if (driver_version == CAS_PROTO_MAKE_VER (PROTOCOL_V2) || di_understand_renewed_error_code (driver_info))
	{
	  write_val = htonl (error);
	}
      else
	{
	  write_val = htonl (CAS_CONV_ERROR_TO_OLD (error));
	}
    }

  write_to_client (sock, (char *) &write_val, sizeof (int));
}

static const char *cas_client_type_str[] = {
  "UNKNOWN",			/* CAS_CLIENT_NONE */
  "CCI",			/* CAS_CLIENT_CCI */
  "ODBC",			/* CAS_CLIENT_ODBC */
  "JDBC",			/* CAS_CLIENT_JDBC */
  "PHP",			/* CAS_CLIENT_PHP */
  "OLEDB",			/* CAS_CLIENT_OLEDB */
  "INTERNAL_JDBC",		/* CAS_CLIENT_SERVER_SIDE_JDBC */
  "GATEWAY_CCI"			/* CAS_CLIENT_GATEWAY */
};

static THREAD_FUNC
receiver_thr_f (void *arg)
{
  T_SOCKLEN clt_sock_addr_len;
  struct sockaddr_in clt_sock_addr;
  SOCKET clt_sock_fd;
  int job_queue_size;
  T_MAX_HEAP_NODE *job_queue;
  T_MAX_HEAP_NODE new_job;
  int job_count;
  int read_len;
  int one = 1;
  char cas_req_header[SRV_CON_CLIENT_INFO_SIZE];
  char cas_client_type;
  char driver_version;
  T_BROKER_VERSION client_version;
#if defined(LINUX)
  int timeout;
#endif /* LINUX */

  job_queue_size = shm_appl->job_queue_size;
  job_queue = shm_appl->job_queue;
  job_count = 1;

#if !defined(WINDOWS)
  signal (SIGPIPE, SIG_IGN);
#endif

#if defined(LINUX)
  timeout = 5;
  setsockopt (sock_fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, (char *) &timeout, sizeof (timeout));
#endif /* LINUX */

  while (process_flag)
    {
      clt_sock_addr_len = sizeof (clt_sock_addr);
      clt_sock_fd = accept (sock_fd, (struct sockaddr *) &clt_sock_addr, &clt_sock_addr_len);
      if (IS_INVALID_SOCKET (clt_sock_fd))
	{
	  continue;
	}

      if (shm_br->br_info[br_index].monitor_hang_flag && shm_br->br_info[br_index].reject_client_flag)
	{
	  shm_br->br_info[br_index].reject_client_count++;
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

#if !defined(WINDOWS) && defined(ASYNC_MODE)
      if (fcntl (clt_sock_fd, F_SETFL, FNDELAY) < 0)
	{
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}
#endif

      setsockopt (clt_sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &one, sizeof (one));
      ut_set_keepalive (clt_sock_fd);

      cas_client_type = CAS_CLIENT_NONE;

      /* read header */
      read_len = read_nbytes_from_client (clt_sock_fd, cas_req_header, SRV_CON_CLIENT_INFO_SIZE);
      if (read_len < 0)
	{
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      if (strncmp (cas_req_header, "PING", 4) == 0)
	{
	  int ret_code = 0;
	  CAS_SEND_ERROR_CODE (clt_sock_fd, ret_code);
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      if (strncmp (cas_req_header, "ST", 2) == 0)
	{
	  int status = FN_STATUS_NONE;
	  int pid, i;
	  unsigned int session_id;

	  memcpy ((char *) &pid, cas_req_header + 2, 4);
	  pid = ntohl (pid);
	  memcpy ((char *) &session_id, cas_req_header + 6, 4);
	  session_id = ntohl (session_id);

	  if (shm_br->br_info[br_index].shard_flag == ON)
	    {
	      status = FN_STATUS_BUSY;
	    }
	  else
	    {
	      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
		{
		  if (shm_appl->as_info[i].service_flag == SERVICE_ON && shm_appl->as_info[i].pid == pid)
		    {
		      if (session_id == shm_appl->as_info[i].session_id)
			{
			  status = shm_appl->as_info[i].fn_status;
			}
		      break;
		    }
		}
	    }

	  CAS_SEND_ERROR_CODE (clt_sock_fd, status);
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      /*
       * Query cancel message (size in bytes)
       *
       * - For client version 8.4.0 patch 1 or below:
       *   |COMMAND("CANCEL",6)|PID(4)|
       *
       * - For CAS protocol version 1 or above:
       *   |COMMAND("QC",2)|PID(4)|CLIENT_PORT(2)|RESERVED(2)|
       *
       *   CLIENT_PORT can be 0 if the client failed to get its local port.
       */
      else if (strncmp (cas_req_header, "QC", 2) == 0 || strncmp (cas_req_header, "CANCEL", 6) == 0
	       || strncmp (cas_req_header, "X1", 2) == 0)
	{
	  int ret_code = 0;
#if !defined(WINDOWS)
	  int pid, i;
	  unsigned short client_port = 0;
#endif

#if !defined(WINDOWS)
	  if (cas_req_header[0] == 'Q')
	    {
	      memcpy ((char *) &pid, cas_req_header + 2, 4);
	      memcpy ((char *) &client_port, cas_req_header + 6, 2);
	      pid = ntohl (pid);
	      client_port = ntohs (client_port);
	    }
	  else
	    {
	      memcpy ((char *) &pid, cas_req_header + 6, 4);
	      pid = ntohl (pid);
	    }

	  ret_code = CAS_ER_QUERY_CANCEL;
	  if (shm_br->br_info[br_index].shard_flag == OFF)
	    {

	      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
		{
		  if (shm_appl->as_info[i].service_flag == SERVICE_ON && shm_appl->as_info[i].pid == pid
		      && shm_appl->as_info[i].uts_status == UTS_STATUS_BUSY)
		    {
		      if (cas_req_header[0] == 'Q' && client_port > 0
			  && shm_appl->as_info[i].cas_clt_port != client_port
			  && memcmp (&shm_appl->as_info[i].cas_clt_ip, &clt_sock_addr.sin_addr, 4) != 0)
			{
			  continue;
			}

		      ret_code = 0;
		      kill (pid, SIGUSR1);
		      break;
		    }
		}
	    }
	  else
	    {
	      /* SHARD TODO : not implemented yet */
	    }
#endif
	  if (cas_req_header[0] == 'X')
	    {
	      char driver_info[SRV_CON_CLIENT_INFO_SIZE];

	      driver_info[SRV_CON_MSG_IDX_PROTO_VERSION] = cas_req_header[2];
	      driver_info[SRV_CON_MSG_IDX_FUNCTION_FLAG] = cas_req_header[3];
	      send_error_to_driver (clt_sock_fd, ret_code, driver_info);
	    }
	  else
	    {
	      ret_code = CAS_CONV_ERROR_TO_OLD (ret_code);
	      CAS_SEND_ERROR_CODE (clt_sock_fd, ret_code);
	    }
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      cas_client_type = cas_req_header[SRV_CON_MSG_IDX_CLIENT_TYPE];
      if (!(strncmp (cas_req_header, SRV_CON_CLIENT_MAGIC_STR, SRV_CON_CLIENT_MAGIC_LEN) == 0
	    || strncmp (cas_req_header, SRV_CON_CLIENT_MAGIC_STR_SSL, SRV_CON_CLIENT_MAGIC_LEN) == 0)
	  || cas_client_type < CAS_CLIENT_TYPE_MIN || cas_client_type > CAS_CLIENT_TYPE_MAX)
	{
	  send_error_to_driver (clt_sock_fd, CAS_ER_NOT_AUTHORIZED_CLIENT, cas_req_header);
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      if ((IS_SSL_CLIENT (cas_req_header) && shm_br->br_info[br_index].use_SSL == OFF)
	  || (!IS_SSL_CLIENT (cas_req_header) && shm_br->br_info[br_index].use_SSL == ON))
	{
	  send_error_to_driver (clt_sock_fd, CAS_ER_SSL_TYPE_NOT_ALLOWED, cas_req_header);
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      driver_version = cas_req_header[SRV_CON_MSG_IDX_PROTO_VERSION];
      if (driver_version & CAS_PROTO_INDICATOR)
	{
	  /* Protocol version */
	  client_version = CAS_PROTO_UNPACK_NET_VER (driver_version);
	}
      else
	{
	  /* Build version; major, minor, and patch */
	  client_version =
	    CAS_MAKE_VER (cas_req_header[SRV_CON_MSG_IDX_MAJOR_VER], cas_req_header[SRV_CON_MSG_IDX_MINOR_VER],
			  cas_req_header[SRV_CON_MSG_IDX_PATCH_VER]);
	}

      if (v3_acl != NULL)
	{
	  unsigned char ip_addr[4];

	  memcpy (ip_addr, &(clt_sock_addr.sin_addr), 4);

	  if (uw_acl_check (ip_addr) < 0)
	    {
	      send_error_to_driver (clt_sock_fd, CAS_ER_NOT_AUTHORIZED_CLIENT, cas_req_header);
	      CLOSE_SOCKET (clt_sock_fd);
	      continue;
	    }
	}

      if (job_queue[0].id == job_queue_size)
	{
	  send_error_to_driver (clt_sock_fd, CAS_ER_FREE_SERVER, cas_req_header);
	  CLOSE_SOCKET (clt_sock_fd);
	  continue;
	}

      if (max_open_fd < clt_sock_fd)
	{
	  max_open_fd = clt_sock_fd;
	}

      job_count = (job_count >= JOB_COUNT_MAX) ? 1 : job_count + 1;
      new_job.id = job_count;
      new_job.clt_sock_fd = clt_sock_fd;
      new_job.recv_time = time (NULL);
      new_job.priority = 0;
      new_job.script[0] = '\0';
      new_job.cas_client_type = cas_client_type;
      new_job.port = ntohs (clt_sock_addr.sin_port);
      memcpy (new_job.ip_addr, &(clt_sock_addr.sin_addr), 4);
      strcpy (new_job.prg_name, cas_client_type_str[(int) cas_client_type]);
      new_job.clt_version = client_version;
      memcpy (new_job.driver_info, cas_req_header, SRV_CON_CLIENT_INFO_SIZE);

      while (1)
	{
	  pthread_mutex_lock (&clt_table_mutex);
	  if (max_heap_insert (job_queue, job_queue_size, &new_job) < 0)
	    {
	      pthread_mutex_unlock (&clt_table_mutex);
	      SLEEP_MILISEC (0, 100);
	    }
	  else
	    {
	      pthread_cond_signal (&clt_table_cond);
	      pthread_mutex_unlock (&clt_table_mutex);
	      break;
	    }
	}
    }

#if defined(WINDOWS)
  return;
#else
  return NULL;
#endif
}

static THREAD_FUNC
dispatch_thr_f (void *arg)
{
  T_MAX_HEAP_NODE *job_queue;
  T_MAX_HEAP_NODE cur_job;
#if !defined(WINDOWS)
  SOCKET srv_sock_fd;
#endif /* !WINDOWS */

  int as_index, i;

  job_queue = shm_appl->job_queue;

  while (process_flag)
    {
      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	{
	  if (shm_appl->as_info[i].service_flag == SERVICE_OFF)
	    {
	      if (shm_appl->as_info[i].uts_status == UTS_STATUS_IDLE)
		shm_appl->as_info[i].service_flag = SERVICE_OFF_ACK;
	    }
	}

      pthread_mutex_lock (&clt_table_mutex);
      if (max_heap_delete (job_queue, &cur_job) < 0)
	{
	  struct timespec ts;
	  struct timeval tv;
	  int r;

	  gettimeofday (&tv, NULL);
	  ts.tv_sec = tv.tv_sec;
	  ts.tv_nsec = (tv.tv_usec + 30000) * 1000;
	  if (ts.tv_nsec > 1000000000)
	    {
	      ts.tv_sec += 1;
	      ts.tv_nsec -= 1000000000;
	    }
	  r = pthread_cond_timedwait (&clt_table_cond, &clt_table_mutex, &ts);
	  if (r != 0)
	    {
	      pthread_mutex_unlock (&clt_table_mutex);
	      continue;
	    }
	  r = max_heap_delete (job_queue, &cur_job);
	  assert (r == 0);
	}

      hold_job = 1;
      max_heap_incr_priority (job_queue);
      pthread_mutex_unlock (&clt_table_mutex);

#if !defined (WINDOWS)
    retry:
#endif
      while (1)
	{
	  as_index = find_idle_cas ();
	  if (as_index < 0)
	    {
	      if (broker_add_new_cas ())
		{
		  continue;
		}
	      else
		{
		  SLEEP_MILISEC (0, 30);
		}
	    }
	  else
	    {
	      break;
	    }
	}

      hold_job = 0;

      shm_appl->as_info[as_index].num_connect_requests++;
#if !defined(WIN_FW)
      shm_appl->as_info[as_index].clt_version = cur_job.clt_version;
      memcpy (shm_appl->as_info[as_index].driver_info, cur_job.driver_info, SRV_CON_CLIENT_INFO_SIZE);
      shm_appl->as_info[as_index].cas_client_type = cur_job.cas_client_type;
      memcpy (shm_appl->as_info[as_index].cas_clt_ip, cur_job.ip_addr, 4);
      shm_appl->as_info[as_index].cas_clt_port = cur_job.port;
#if defined(WINDOWS)
      shm_appl->as_info[as_index].uts_status = UTS_STATUS_BUSY_WAIT;
      CAS_SEND_ERROR_CODE (cur_job.clt_sock_fd, shm_appl->as_info[as_index].as_port);
      CLOSE_SOCKET (cur_job.clt_sock_fd);
      shm_appl->as_info[as_index].num_request++;
      shm_appl->as_info[as_index].last_access_time = time (NULL);
      shm_appl->as_info[as_index].transaction_start_time = (time_t) 0;
#else /* WINDOWS */

      srv_sock_fd = connect_srv (shm_br->br_info[br_index].name, as_index);

      if (!IS_INVALID_SOCKET (srv_sock_fd))
	{
	  int ip_addr;
	  int ret_val;
	  int con_status, uts_status;

	  con_status = htonl (shm_appl->as_info[as_index].con_status);

	  ret_val = write_to_client_with_timeout (srv_sock_fd, (char *) &con_status, sizeof (int), SOCKET_TIMEOUT_SEC);
	  if (ret_val != sizeof (int))
	    {
	      CLOSE_SOCKET (srv_sock_fd);
	      goto retry;
	    }

	  ret_val = read_from_client_with_timeout (srv_sock_fd, (char *) &con_status, sizeof (int), SOCKET_TIMEOUT_SEC);
	  if (ret_val != sizeof (int) || ntohl (con_status) != CON_STATUS_IN_TRAN)
	    {
	      CLOSE_SOCKET (srv_sock_fd);
	      goto retry;
	    }

	  memcpy (&ip_addr, cur_job.ip_addr, 4);
	  ret_val = send_fd (srv_sock_fd, cur_job.clt_sock_fd, ip_addr, cur_job.driver_info);
	  if (ret_val > 0)
	    {
	      ret_val =
		read_from_client_with_timeout (srv_sock_fd, (char *) &uts_status, sizeof (int), SOCKET_TIMEOUT_SEC);
	    }
	  CLOSE_SOCKET (srv_sock_fd);

	  if (ret_val < 0)
	    {
	      send_error_to_driver (cur_job.clt_sock_fd, CAS_ER_FREE_SERVER, cur_job.driver_info);
	    }
	  else
	    {
	      shm_appl->as_info[as_index].num_request++;
	    }
	}
      else
	{
	  goto retry;
	}

      CLOSE_SOCKET (cur_job.clt_sock_fd);
#endif /* ifdef !WINDOWS */
#else /* !WIN_FW */
      session_request_q[as_index] = cur_job;
#endif /* WIN_FW */
    }

#if defined(WINDOWS)
  return;
#else
  return NULL;
#endif
}

#if defined(WIN_FW)
static THREAD_FUNC
service_thr_f (void *arg)
{
  int self_index = *((int *) arg);
  SOCKET clt_sock_fd, srv_sock_fd;
  int ip_addr;
  int cas_pid;
  T_MAX_HEAP_NODE cur_job;

  while (process_flag)
    {
      if (!IS_INVALID_SOCKET (session_request_q[self_index].clt_sock_fd))
	{
	  cur_job = session_request_q[self_index];
	  session_request_q[self_index].clt_sock_fd = INVALID_SOCKET;
	}
      else
	{
	  SLEEP_MILISEC (0, 10);
	  continue;
	}

      clt_sock_fd = cur_job.clt_sock_fd;
      memcpy (&ip_addr, cur_job.ip_addr, 4);

      shm_appl->as_info[self_index].clt_major_version = cur_job.clt_major_version;
      shm_appl->as_info[self_index].clt_minor_version = cur_job.clt_minor_version;
      shm_appl->as_info[self_index].clt_patch_version = cur_job.clt_patch_version;
      shm_appl->as_info[self_index].cas_client_type = cur_job.cas_client_type;
      shm_appl->as_info[self_index].close_flag = 0;
      cas_pid = shm_appl->as_info[self_index].pid;

      srv_sock_fd = connect_srv (shm_br->br_info[br_index].name, self_index);
      if (IS_INVALID_SOCKET (srv_sock_fd))
	{
	  send_error_to_driver (clt_sock_fd, CAS_ER_FREE_SERVER, cur_job.driver_info);
	  shm_appl->as_info[self_index].uts_status = UTS_STATUS_IDLE;
	  CLOSE_SOCKET (cur_job.clt_sock_fd);
	  continue;
	}
      else
	{
	  CAS_SEND_ERROR_CODE (clt_sock_fd, 0);
	  shm_appl->as_info[self_index].num_request++;
	  shm_appl->as_info[self_index].last_access_time = time (NULL);
	  shm_appl->as_info[self_index].transaction_start_time = (time_t) 0;
	}

      process_cas_request (cas_pid, self_index, clt_sock_fd, srv_sock_fd, cur_job.clt_major_version);

      CLOSE_SOCKET (clt_sock_fd);
      CLOSE_SOCKET (srv_sock_fd);
    }

  return;
}
#endif

static int
init_env (void)
{
  char *port;
  int n;
  int one = 1;

  /* get a Unix stream socket */
  sock_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sock_fd))
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, errno);
      return (-1);
    }
  if ((setsockopt (sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof (one))) < 0)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, errno);
      return (-1);
    }
  if ((port = getenv (PORT_NUMBER_ENV_STR)) == NULL)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, 0);
      return (-1);
    }
  memset (&sock_addr, 0, sizeof (struct sockaddr_in));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons ((unsigned short) (atoi (port)));
  sock_addr_len = sizeof (struct sockaddr_in);

  n = INADDR_ANY;
  memcpy (&sock_addr.sin_addr, &n, sizeof (int));

  if (bind (sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len) < 0)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_BIND, errno);
      return (-1);
    }

#if defined (WINDOWS)
  if (listen (sock_fd, SOMAXCONN_HINT (shm_appl->job_queue_size)) < 0)
#else
  if (listen (sock_fd, shm_appl->job_queue_size) < 0)
#endif
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_BIND, 0);
      return (-1);
    }

  return (0);
}

static int
read_from_client (SOCKET sock_fd, char *buf, int size)
{
  return read_from_client_with_timeout (sock_fd, buf, size, 60);
}

static int
read_from_client_with_timeout (SOCKET sock_fd, char *buf, int size, int timeout_sec)
{
  int len = size;
  int read_len;

  if (IS_INVALID_SOCKET (sock_fd))
    {
      return -1;
    }

#ifdef ASYNC_MODE
  while (size > 0)
    {
      read_len = read_buffer_async (sock_fd, buf, size, timeout_sec);
      if (read_len <= 0)
	{
	  return -1;
	}

      buf += read_len;
      size -= read_len;
    }
#else
  read_len = READ_FROM_SOCKET (sock_fd, buf, size);
#endif

  return len;
}

static int
write_to_client (SOCKET sock_fd, char *buf, int size)
{
  return write_to_client_with_timeout (sock_fd, buf, size, 60);
}

static int
write_to_client_with_timeout (SOCKET sock_fd, char *buf, int size, int timeout_sec)
{
  int len = size;
  int write_len = -1;

  if (IS_INVALID_SOCKET (sock_fd))
    {
      return -1;
    }

#ifdef ASYNC_MODE
  while (size > 0)
    {
      write_len = write_buffer_async (sock_fd, buf, size, timeout_sec);

      if (write_len <= 0)
	{
	  return -1;
	}

      buf += write_len;
      size -= write_len;
    }
#else
  len = WRITE_TO_SOCKET (sock_fd, buf, size);
#endif

  return len;
}

/*
 * run_appl_server () -
 *   return: pid
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   proxy_index(in): it's only valid in SHARD! proxy index
 *   shard_index(in): it's only valid in SHARD! shard index
 *   as_index(in): cas index
 *
 * Note: activate CAS
 */
static int
run_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
  char appl_name[APPL_SERVER_NAME_MAX_SIZE];
  int pid;
  char argv0[128];
#if !defined(WINDOWS)
  int i;
#endif
  char as_id_env_str[32];
  char appl_server_shm_key_env_str[32];

  while (1)
    {
      pthread_mutex_lock (&run_appl_mutex);
      if (run_appl_server_flag)
	{
	  pthread_mutex_unlock (&run_appl_mutex);
	  SLEEP_MILISEC (0, 100);
	  continue;
	}
      else
	{
	  run_appl_server_flag = 1;
	  pthread_mutex_unlock (&run_appl_mutex);
	  break;
	}
    }

  as_info_p->service_ready_flag = FALSE;

#if !defined(WINDOWS)
  signal (SIGCHLD, SIG_IGN);

  {
    char path[BROKER_PATH_MAX];

    ut_get_as_port_name (path, shm_br->br_info[br_index].name, as_index, BROKER_PATH_MAX);
    unlink (path);
  }

  pid = fork ();
  if (pid == 0)
    {
      signal (SIGCHLD, SIG_DFL);

      for (i = 3; i <= max_open_fd; i++)
	{
	  close (i);
	}
#endif
      strcpy (appl_name, shm_appl->appl_server_name);

      sprintf (appl_server_shm_key_env_str, "%s=%d", APPL_SERVER_SHM_KEY_STR,
	       shm_br->br_info[br_index].appl_server_shm_id);
      putenv (appl_server_shm_key_env_str);

      snprintf (as_id_env_str, sizeof (as_id_env_str), "%s=%d", AS_ID_ENV_STR, as_index);
      putenv (as_id_env_str);

      snprintf (argv0, sizeof (argv0) - 1, "%s_%s_%d", shm_br->br_info[br_index].name, appl_name, as_index + 1);

#if defined(WINDOWS)
      pid = run_child (appl_name);
#else
      execle (appl_name, argv0, NULL, environ);
#endif

#if !defined(WINDOWS)
      exit (0);
    }
#endif

  CON_STATUS_LOCK_DESTROY (as_info_p);
  CON_STATUS_LOCK_INIT (as_info_p);
  if (ut_is_appl_server_ready (pid, &as_info_p->service_ready_flag))
    {
      as_info_p->transaction_start_time = (time_t) 0;
      as_info_p->num_restarts++;
    }
  else
    {
      pid = -1;
    }

  run_appl_server_flag = 0;

  return pid;
}

/*
 * stop_appl_server () -
 *   return: NO_ERROR
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: inactivate CAS
 */
static int
stop_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
  ut_kill_as_process (as_info_p->pid, shm_br->br_info[br_index].name, as_index, OFF);

#if defined(WINDOWS)
  /* [CUBRIDSUS-2068] make the broker sleep for 0.1 sec when stopping the cas in order to prevent communication error
   * occurred on windows. */
  SLEEP_MILISEC (0, 100);
#endif

  as_info_p->pid = 0;
  as_info_p->last_access_time = time (NULL);
  as_info_p->transaction_start_time = (time_t) 0;

  return 0;
}

/*
 * restart_appl_server () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: inactivate and activate CAS
 */
static void
restart_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
  int new_pid;

#if defined(WINDOWS)
  ut_kill_as_process (as_info_p->pid, shm_br->br_info[br_index].name, as_info_p->as_id, OFF);

  /* [CUBRIDSUS-2068] make the broker sleep for 0.1 sec when stopping the cas in order to prevent communication error
   * occurred on windows. */

  SLEEP_MILISEC (0, 100);

  new_pid = run_appl_server (as_info_p, br_index, as_index);
  as_info_p->pid = new_pid;
#else

  as_info_p->psize = getsize (as_info_p->pid);
  if (as_info_p->psize > 1)
    {
      as_info_p->psize_time = time (NULL);
    }
  else
    {
      char pid_file_name[BROKER_PATH_MAX];
      FILE *fp;
      int old_pid;

      ut_get_as_pid_name (pid_file_name, shm_br->br_info[br_index].name, as_index, BROKER_PATH_MAX);

      fp = fopen (pid_file_name, "r");
      if (fp)
	{
	  fscanf (fp, "%d", &old_pid);
	  fclose (fp);

	  as_info_p->psize = getsize (old_pid);
	  if (as_info_p->psize > 1)
	    {
	      as_info_p->pid = old_pid;
	      as_info_p->psize_time = time (NULL);
	    }
	  else
	    {
	      unlink (pid_file_name);
	    }
	}
    }

  if (as_info_p->psize <= 0)
    {
      if (as_info_p->pid > 0)
	{
	  ut_kill_as_process (as_info_p->pid, shm_br->br_info[br_index].name, as_index, OFF);
	}

      new_pid = run_appl_server (as_info_p, br_index, as_index);
      as_info_p->pid = new_pid;
    }
#endif
}

static int
read_nbytes_from_client (SOCKET sock_fd, char *buf, int size)
{
  int total_read_size = 0, read_len;

  while (total_read_size < size)
    {
      read_len = read_from_client (sock_fd, buf + total_read_size, size - total_read_size);
      if (read_len <= 0)
	{
	  total_read_size = -1;
	  break;
	}
      total_read_size += read_len;
    }
  return total_read_size;
}

static SOCKET
connect_srv (char *br_name, int as_index)
{
  int sock_addr_len;
#if defined(WINDOWS)
  struct sockaddr_in sock_addr;
#else
  struct sockaddr_un sock_addr;
#endif
  SOCKET srv_sock_fd;
  int one = 1;
  char retry_count = 0;

retry:

#if defined(WINDOWS)
  srv_sock_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (srv_sock_fd))
    return INVALID_SOCKET;

  memset (&sock_addr, 0, sizeof (struct sockaddr_in));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons ((unsigned short) shm_appl->as_info[as_index].as_port);
  memcpy (&sock_addr.sin_addr, shm_br->my_ip_addr, 4);
  sock_addr_len = sizeof (struct sockaddr_in);
#else
  srv_sock_fd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (srv_sock_fd))
    return INVALID_SOCKET;

  memset (&sock_addr, 0, sizeof (struct sockaddr_un));
  sock_addr.sun_family = AF_UNIX;

  ut_get_as_port_name (sock_addr.sun_path, br_name, as_index, sizeof (sock_addr.sun_path));

  sock_addr_len = strlen (sock_addr.sun_path) + sizeof (sock_addr.sun_family) + 1;
#endif

  if (connect (srv_sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len) < 0)
    {
      if (retry_count < 1)
	{
	  int new_pid;

	  ut_kill_as_process (shm_appl->as_info[as_index].pid, shm_br->br_info[br_index].name, as_index, OFF);

	  new_pid = run_appl_server (&(shm_appl->as_info[as_index]), br_index, as_index);
	  shm_appl->as_info[as_index].pid = new_pid;
	  retry_count++;
	  CLOSE_SOCKET (srv_sock_fd);
	  goto retry;
	}
      CLOSE_SOCKET (srv_sock_fd);
      return INVALID_SOCKET;
    }

  setsockopt (srv_sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &one, sizeof (one));

  return srv_sock_fd;
}

/*
 * cas_monitor_worker () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *   busy_uts(out): counting UTS_STATUS_BUSY status cas
 *
 * Note: monitoring CAS
 */
static void
cas_monitor_worker (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index, int *busy_uts)
{
  int new_pid;
  int restart_flag = OFF;
  T_PROXY_INFO *proxy_info_p = NULL;
  T_SHARD_INFO *shard_info_p = NULL;

  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }
  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      (*busy_uts)++;
    }
#if defined(WINDOWS)
  else if (as_info_p->uts_status == UTS_STATUS_BUSY_WAIT)
    {
      if (time (NULL) - as_info_p->last_access_time > 10)
	{
	  as_info_p->uts_status = UTS_STATUS_IDLE;
	}
      else
	{
	  (*busy_uts)++;
	}
    }
#endif

#if defined(WINDOWS)
  if (shm_appl->use_pdh_flag == TRUE)
    {
      if ((as_info_p->pid == as_info_p->pdh_pid)
	  && (as_info_p->pdh_workset > shm_br->br_info[br_index].appl_server_hard_limit))
	{
	  as_info_p->uts_status = UTS_STATUS_RESTART;
	}
    }
#else
  if (as_info_p->psize > shm_appl->appl_server_hard_limit)
    {
      as_info_p->uts_status = UTS_STATUS_RESTART;
    }
#endif

/*  if (as_info_p->service_flag != SERVICE_ON)
        continue;  */
  /* check cas process status and restart it */

  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      restart_flag = ON;
    }

  if (restart_flag)
    {
#if defined(WINDOWS)
      HANDLE phandle;
      phandle = OpenProcess (SYNCHRONIZE, FALSE, as_info_p->pid);
      if (phandle == NULL)
	{
	  restart_appl_server (as_info_p, br_index, as_index);
	  as_info_p->uts_status = UTS_STATUS_IDLE;
	}
      else
	{
	  CloseHandle (phandle);
	}
#else
      if (kill (as_info_p->pid, 0) < 0)
	{
	  restart_appl_server (as_info_p, br_index, as_index);
	  as_info_p->uts_status = UTS_STATUS_IDLE;
	}
#endif
    }

  if (as_info_p->uts_status == UTS_STATUS_RESTART)
    {
      stop_appl_server (as_info_p, br_index, as_index);
      new_pid = run_appl_server (as_info_p, br_index, as_index);

      as_info_p->pid = new_pid;
      as_info_p->uts_status = UTS_STATUS_IDLE;
    }
}

static THREAD_FUNC
cas_monitor_thr_f (void *ar)
{
  int i, tmp_num_busy_uts;

  while (process_flag)
    {
      tmp_num_busy_uts = 0;
      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	{
	  cas_monitor_worker (&(shm_appl->as_info[i]), br_index, i, &tmp_num_busy_uts);
	}

      num_busy_uts = tmp_num_busy_uts;
      shm_br->br_info[br_index].num_busy_count = num_busy_uts;
      SLEEP_MILISEC (0, 100);
    }

#if !defined(WINDOWS)
  return NULL;
#endif
}

static int
insert_db_server_check_list (T_DB_SERVER * list_p, int check_list_cnt, const char *db_name, const char *db_host)
{
  int i;

  for (i = 0; i < check_list_cnt && i < UNUSABLE_DATABASE_MAX; i++)
    {
      if (strcmp (db_name, list_p[i].database_name) == 0 && strcmp (db_host, list_p[i].database_host) == 0)
	{
	  return check_list_cnt;
	}
    }

  if (i == UNUSABLE_DATABASE_MAX)
    {
      return UNUSABLE_DATABASE_MAX;
    }

  strncpy_bufsize (list_p[i].database_name, db_name);
  strncpy_bufsize (list_p[i].database_host, db_host);
  list_p[i].state = -1;

  return i + 1;
}

static THREAD_FUNC
hang_check_thr_f (void *ar)
{
  unsigned int cur_index;
  int cur_hang_count;
  T_BROKER_INFO *br_info_p;
  time_t cur_time;
  int collect_count_interval;
  int hang_count[NUM_COLLECT_COUNT_PER_INTVL] = { 0, 0, 0, 0 };
  float avg_hang_count;

  int proxy_index, i;
  T_PROXY_INFO *proxy_info_p = NULL;
  T_APPL_SERVER_INFO *as_info_p;

  SLEEP_MILISEC (shm_br->br_info[br_index].monitor_hang_interval, 0);

  br_info_p = &(shm_br->br_info[br_index]);
  cur_hang_count = 0;
  cur_index = 0;
  avg_hang_count = 0.0;
  collect_count_interval = br_info_p->monitor_hang_interval / NUM_COLLECT_COUNT_PER_INTVL;

  while (process_flag)
    {
      cur_time = time (NULL);
      {
	for (i = 0; i < br_info_p->appl_server_max_num; i++)
	  {
	    as_info_p = &(shm_appl->as_info[i]);

	    if ((as_info_p->service_flag != SERVICE_ON) || as_info_p->claimed_alive_time == 0)
	      {
		continue;
	      }
	    if ((br_info_p->hang_timeout < cur_time - as_info_p->claimed_alive_time))
	      {
		cur_hang_count++;
	      }
	  }
      }

      hang_count[cur_index] = cur_hang_count;

      avg_hang_count = ut_get_avg_from_array (hang_count, NUM_COLLECT_COUNT_PER_INTVL);

      {
	br_info_p->reject_client_flag =
	  (avg_hang_count >= (float) br_info_p->appl_server_num * HANG_COUNT_THRESHOLD_RATIO);
      }

      cur_index = (cur_index + 1) % NUM_COLLECT_COUNT_PER_INTVL;
      cur_hang_count = 0;

      SLEEP_MILISEC (collect_count_interval, 0);
    }
#if !defined(WINDOWS)
  return NULL;
#endif
}

/*
 * psize_check_worker () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   proxy_index(in): it's only valid in SHARD! proxy index
 *   shard_index(in): it's only valid in SHARD! shard index
 *   as_index(in): cas index
 *
 * Note: check cas psize and cas log
 */
static void
psize_check_worker (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
#if defined(WINDOWS)
  int pid;
  int cpu_time;
  int workset_size;
  float pct_cpu;
#endif

  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }

#if defined(WINDOWS)
  pid = as_info_p->pid;

  cpu_time = get_cputime_sec (pid);
  if (cpu_time < 0)
    {
      as_info_p->cpu_time = 0;
#if 0
      HANDLE hProcess;
      hProcess = OpenProcess (PROCESS_QUERY_INFORMATION, FALSE, pid);
      if (hProcess == NULL)
	{
	  pid = 0;
	  as_info_p->pid = 0;
	  as_info_p->cpu_time = 0;
	}
#endif
    }
  else
    {
      as_info_p->cpu_time = cpu_time;
    }

  if (pdh_get_value (pid, &workset_size, &pct_cpu, NULL) >= 0)
    {
      as_info_p->pdh_pid = pid;
      as_info_p->pdh_workset = workset_size;
      as_info_p->pdh_pct_cpu = pct_cpu;
    }
#else
  as_info_p->psize = getsize (as_info_p->pid);
#if 0
  if (as_info_p->psize < 0 && as_info_p->pid > 0)
    {
      if (kill (as_info_p->pid, 0) < 0 && errno == ESRCH)
	{
	  as_info_p->pid = 0;
	}
    }
#endif
#endif /* WINDOWS */

  check_cas_log (shm_br->br_info[br_index].name, as_info_p, as_index);
}

static void
check_proxy_log (char *br_name, T_PROXY_INFO * proxy_info_p)
{
  char log_filepath[BROKER_PATH_MAX];

  if (proxy_info_p->cur_proxy_log_mode != PROXY_LOG_MODE_NONE)
    {
      snprintf (log_filepath, sizeof (log_filepath), "%s/%s_%d.log", shm_appl->proxy_log_dir, br_name,
		proxy_info_p->proxy_id + 1);

      if (access (log_filepath, F_OK) < 0)
	{
	  FILE *fp;

	  fp = fopen (log_filepath, "a");
	  if (fp != NULL)
	    {
	      fclose (fp);
	    }
	  proxy_info_p->proxy_log_reset = PROXY_LOG_RESET_REOPEN;
	}
    }

  return;
}

static void
check_proxy_access_log (T_PROXY_INFO * proxy_info_p)
{
  char *access_log_file;
  FILE *fp;

  access_log_file = proxy_info_p->access_log_file;
  if (access (access_log_file, F_OK) < 0)
    {
      fp = fopen (access_log_file, "a");
      if (fp != NULL)
	{
	  fclose (fp);
	}
      proxy_info_p->proxy_access_log_reset = PROXY_LOG_RESET_REOPEN;
    }

  return;
}


static void
proxy_check_worker (int br_index, T_PROXY_INFO * proxy_info_p)
{
  check_proxy_log (shm_br->br_info[br_index].name, proxy_info_p);
  check_proxy_access_log (proxy_info_p);

  return;
}

#if defined(WINDOWS)
static int
get_cputime_sec (int pid)
{
  ULARGE_INTEGER ul;
  HANDLE hProcess;
  FILETIME ctime, etime, systime, usertime;
  int cputime = 0;

  if (pid <= 0)
    return 0;

  hProcess = OpenProcess (PROCESS_QUERY_INFORMATION, FALSE, pid);
  if (hProcess == NULL)
    {
      return -1;
    }

  if (GetProcessTimes (hProcess, &ctime, &etime, &systime, &usertime) != 0)
    {
      ul.HighPart = systime.dwHighDateTime + usertime.dwHighDateTime;
      ul.LowPart = systime.dwLowDateTime + usertime.dwLowDateTime;
      cputime = ((int) (ul.QuadPart / 10000000));
    }
  CloseHandle (hProcess);

  return cputime;
}

static THREAD_FUNC
psize_check_thr_f (void *ar)
{
  int workset_size;
  float pct_cpu;
  int cpu_time;
  int br_num_thr;
  int i;
  int proxy_index;

  T_PROXY_INFO *proxy_info_p = NULL;
  T_SHARD_INFO *shard_info_p = NULL;

  if (pdh_init () < 0)
    {
      shm_appl->use_pdh_flag = FALSE;
      return;
    }
  else
    {
      shm_appl->use_pdh_flag = TRUE;
    }

  while (process_flag)
    {
      pdh_collect ();

      if (pdh_get_value (shm_br->br_info[br_index].pid, &workset_size, &pct_cpu, &br_num_thr) < 0)
	{
	  shm_br->br_info[br_index].pdh_pct_cpu = 0;
	}
      else
	{
	  cpu_time = get_cputime_sec (shm_br->br_info[br_index].pid);
	  if (cpu_time >= 0)
	    {
	      shm_br->br_info[br_index].cpu_time = cpu_time;
	    }
	  shm_br->br_info[br_index].pdh_workset = workset_size;
	  shm_br->br_info[br_index].pdh_pct_cpu = pct_cpu;
	  shm_br->br_info[br_index].pdh_num_thr = br_num_thr;
	}

      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	{
	  psize_check_worker (&(shm_appl->as_info[i]), br_index, i);
	}
      SLEEP_MILISEC (1, 0);
    }
}

#else /* WINDOWS */

static THREAD_FUNC
psize_check_thr_f (void *ar)
{
  int i;
  int proxy_index;

  T_PROXY_INFO *proxy_info_p = NULL;

  while (process_flag)
    {
      for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	{
	  psize_check_worker (&(shm_appl->as_info[i]), br_index, i);
	}

      SLEEP_MILISEC (1, 0);
    }

  return NULL;
}
#endif /* !WINDOWS */

/*
 * check_cas_log () -
 *   return: void
 *   br_name(in): broker name
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   as_index(in): cas index
 * Note: check cas log and recreate
 */
static void
check_cas_log (char *br_name, T_APPL_SERVER_INFO * as_info_p, int as_index)
{
  char log_filename[BROKER_PATH_MAX];

  if (IS_NOT_APPL_SERVER_TYPE_CAS (shm_br->br_info[br_index].appl_server))
    {
      return;
    }

  if (as_info_p->cur_sql_log_mode != SQL_LOG_MODE_NONE)
    {
      get_as_sql_log_filename (log_filename, BROKER_PATH_MAX, br_name, as_info_p, as_index);

      if (access (log_filename, F_OK) < 0)
	{
	  FILE *fp;
	  fp = fopen (log_filename, "a");
	  if (fp != NULL)
	    {
	      fclose (fp);
	    }
	  as_info_p->cas_log_reset = CAS_LOG_RESET_REOPEN;
	}
    }

  if (as_info_p->cur_slow_log_mode != SLOW_LOG_MODE_OFF)
    {
      get_as_slow_log_filename (log_filename, BROKER_PATH_MAX, br_name, as_info_p, as_index);

      if (access (log_filename, F_OK) < 0)
	{
	  FILE *fp;
	  fp = fopen (log_filename, "a");
	  if (fp != NULL)
	    {
	      fclose (fp);
	    }
	  as_info_p->cas_slow_log_reset = CAS_LOG_RESET_REOPEN;
	}
    }
}

#ifdef WIN_FW
static int
process_cas_request (int cas_pid, int as_index, SOCKET clt_sock_fd, SOCKET srv_sock_fd)
{
  char read_buf[1024];
  int msg_size;
  int read_len;
  int tmp_int;
  char *tmp_p;

  msg_size = SRV_CON_DB_INFO_SIZE;
  while (msg_size > 0)
    {
      read_len = read_from_cas_client (clt_sock_fd, read_buf, msg_size, as_index, cas_pid);
      if (read_len <= 0)
	{
	  return -1;
	}
      if (send (srv_sock_fd, read_buf, read_len, 0) < read_len)
	return -1;
      msg_size -= read_len;
    }

  if (recv (srv_sock_fd, (char *) &msg_size, 4, 0) < 4)
    return -1;
  if (write_to_client (clt_sock_fd, (char *) &msg_size, 4) < 0)
    return -1;
  msg_size = ntohl (msg_size);
  while (msg_size > 0)
    {
      read_len = recv (srv_sock_fd, read_buf, (msg_size > sizeof (read_buf) ? sizeof (read_buf) : msg_size), 0);
      if (read_len <= 0)
	{
	  return -1;
	}
      if (write_to_client (clt_sock_fd, read_buf, read_len) < 0)
	{
	  return -1;
	}
      msg_size -= read_len;
    }

  while (1)
    {
      tmp_int = 4;
      tmp_p = (char *) &msg_size;
      while (tmp_int > 0)
	{
	  read_len = read_from_cas_client (clt_sock_fd, tmp_p, tmp_int, as_index, cas_pid);
	  if (read_len <= 0)
	    {
	      return -1;
	    }
	  tmp_int -= read_len;
	  tmp_p += read_len;
	}
      if (send (srv_sock_fd, (char *) &msg_size, 4, 0) < 0)
	{
	  return -1;
	}

      msg_size = ntohl (msg_size);
      while (msg_size > 0)
	{
	  read_len =
	    read_from_cas_client (clt_sock_fd, read_buf, (msg_size > sizeof (read_buf) ? sizeof (read_buf) : msg_size),
				  as_index, cas_pid);
	  if (read_len <= 0)
	    {
	      return -1;
	    }
	  if (send (srv_sock_fd, read_buf, read_len, 0) < read_len)
	    {
	      return -1;
	    }
	  msg_size -= read_len;
	}

      if (recv (srv_sock_fd, (char *) &msg_size, 4, 0) < 4)
	{
	  return -1;
	}
      if (write_to_client (clt_sock_fd, (char *) &msg_size, 4) < 0)
	{
	  return -1;
	}

      msg_size = ntohl (msg_size);
      while (msg_size > 0)
	{
	  read_len = recv (srv_sock_fd, read_buf, (msg_size > sizeof (read_buf) ? sizeof (read_buf) : msg_size), 0);
	  if (read_len <= 0)
	    {
	      return -1;
	    }
	  if (write_to_client (clt_sock_fd, read_buf, read_len) < 0)
	    {
	      return -1;
	    }
	  msg_size -= read_len;
	}

      if (shm_appl->as_info[as_index].close_flag || shm_appl->as_info[as_index].pid != cas_pid)
	{
	  break;
	}
    }

  return 0;
}

static int
read_from_cas_client (SOCKET sock_fd, char *buf, int size, int as_index, int cas_pid)
{
  int read_len;
#ifdef ASYNC_MODE
  SELECT_MASK read_mask;
  int nfound;
  int maxfd;
  struct timeval timeout = { 1, 0 };
#endif

retry:

#ifdef ASYNC_MODE
  FD_ZERO (&read_mask);
  FD_SET (sock_fd, (fd_set *) (&read_mask));
  maxfd = sock_fd + 1;
  nfound = select (maxfd, &read_mask, (SELECT_MASK *) 0, (SELECT_MASK *) 0, &timeout);
  if (nfound < 1)
    {
      if (shm_appl->as_info[as_index].close_flag || shm_appl->as_info[as_index].pid != cas_pid)
	{
	  return -1;
	}
      goto retry;
    }
#endif

#ifdef ASYNC_MODE
  if (FD_ISSET (sock_fd, (fd_set *) (&read_mask)))
    {
#endif
      read_len = READ_FROM_SOCKET (sock_fd, buf, size);
#ifdef ASYNC_MODE
    }
  else
    {
      return -1;
    }
#endif

  return read_len;
}
#endif

static int
find_idle_cas (void)
{
  int i;
  int idle_cas_id = -1;
  time_t max_wait_time;
  int wait_cas_id;
  time_t cur_time = time (NULL);

  pthread_mutex_lock (&broker_shm_mutex);

  wait_cas_id = -1;
  max_wait_time = 0;

  for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
    {
      if (shm_appl->as_info[i].service_flag != SERVICE_ON)
	{
	  continue;
	}
      if (shm_appl->as_info[i].uts_status == UTS_STATUS_IDLE
#if !defined (WINDOWS)
	  && kill (shm_appl->as_info[i].pid, 0) == 0
#endif
	)
	{
	  idle_cas_id = i;
	  wait_cas_id = -1;
	  break;
	}
      if (shm_br->br_info[br_index].appl_server_num == shm_br->br_info[br_index].appl_server_max_num
	  && shm_appl->as_info[i].uts_status == UTS_STATUS_BUSY && shm_appl->as_info[i].cur_keep_con == KEEP_CON_AUTO
	  && shm_appl->as_info[i].con_status == CON_STATUS_OUT_TRAN && shm_appl->as_info[i].num_holdable_results < 1
	  && shm_appl->as_info[i].cas_change_mode == CAS_CHANGE_MODE_AUTO)
	{
	  time_t wait_time = cur_time - shm_appl->as_info[i].last_access_time;
	  if (wait_time > max_wait_time || wait_cas_id == -1)
	    {
	      max_wait_time = wait_time;
	      wait_cas_id = i;
	    }
	}
    }

  if (wait_cas_id >= 0)
    {
      CON_STATUS_LOCK (&(shm_appl->as_info[wait_cas_id]), CON_STATUS_LOCK_BROKER);
      if (shm_appl->as_info[wait_cas_id].con_status == CON_STATUS_OUT_TRAN
	  && shm_appl->as_info[wait_cas_id].num_holdable_results < 1
	  && shm_appl->as_info[wait_cas_id].cas_change_mode == CAS_CHANGE_MODE_AUTO)
	{
	  idle_cas_id = wait_cas_id;
	  shm_appl->as_info[wait_cas_id].con_status = CON_STATUS_CLOSE_AND_CONNECT;
	}
      CON_STATUS_UNLOCK (&(shm_appl->as_info[wait_cas_id]), CON_STATUS_LOCK_BROKER);
    }

#if defined(WINDOWS)
  if (idle_cas_id >= 0)
    {
      HANDLE h_proc;
      h_proc = OpenProcess (SYNCHRONIZE, FALSE, shm_appl->as_info[idle_cas_id].pid);
      if (h_proc == NULL)
	{
	  shm_appl->as_info[i].uts_status = UTS_STATUS_RESTART;
	  idle_cas_id = -1;
	}
      else
	{
	  CloseHandle (h_proc);
	}
    }
#endif

  if (idle_cas_id < 0)
    {
      pthread_mutex_unlock (&broker_shm_mutex);
      return -1;
    }

  shm_appl->as_info[idle_cas_id].uts_status = UTS_STATUS_BUSY;
  pthread_mutex_unlock (&broker_shm_mutex);

  return idle_cas_id;
}

static int
find_drop_as_index (void)
{
  int i, drop_as_index, exist_idle_cas;
  time_t max_wait_time, wait_time;

  pthread_mutex_lock (&broker_shm_mutex);
  if (IS_NOT_APPL_SERVER_TYPE_CAS (shm_br->br_info[br_index].appl_server))
    {
      drop_as_index = shm_br->br_info[br_index].appl_server_num - 1;
      wait_time = time (NULL) - shm_appl->as_info[drop_as_index].last_access_time;
      if (shm_appl->as_info[drop_as_index].uts_status == UTS_STATUS_IDLE
	  && wait_time > shm_br->br_info[br_index].time_to_kill)
	{
	  pthread_mutex_unlock (&broker_shm_mutex);
	  return drop_as_index;
	}
      pthread_mutex_unlock (&broker_shm_mutex);
      return -1;
    }

  drop_as_index = -1;
  max_wait_time = -1;
  exist_idle_cas = 0;

  for (i = shm_br->br_info[br_index].appl_server_max_num - 1; i >= 0; i--)
    {
      if (shm_appl->as_info[i].service_flag != SERVICE_ON)
	continue;

      wait_time = time (NULL) - shm_appl->as_info[i].last_access_time;

      if (shm_appl->as_info[i].uts_status == UTS_STATUS_IDLE)
	{
	  if (wait_time > shm_br->br_info[br_index].time_to_kill)
	    {
	      drop_as_index = i;
	      break;
	    }
	  else
	    {
	      exist_idle_cas = 1;
	      drop_as_index = -1;
	    }
	}

      if (shm_appl->as_info[i].uts_status == UTS_STATUS_BUSY && shm_appl->as_info[i].con_status == CON_STATUS_OUT_TRAN
	  && shm_appl->as_info[i].num_holdable_results < 1
	  && shm_appl->as_info[i].cas_change_mode == CAS_CHANGE_MODE_AUTO && wait_time > max_wait_time
	  && wait_time > shm_br->br_info[br_index].time_to_kill && exist_idle_cas == 0)
	{
	  max_wait_time = wait_time;
	  drop_as_index = i;
	}
    }

  pthread_mutex_unlock (&broker_shm_mutex);

  return drop_as_index;
}

static int
find_add_as_index ()
{
  int i;

  pthread_mutex_lock (&broker_shm_mutex);
  for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
    {
      if (shm_appl->as_info[i].service_flag == SERVICE_OFF_ACK && current_dropping_as_index != i)
	{
	  pthread_mutex_unlock (&broker_shm_mutex);
	  return i;
	}
    }

  pthread_mutex_unlock (&broker_shm_mutex);
  return -1;
}

static int
broker_init_shm (void)
{
  char *p;
  int i;
  int master_shm_key, as_shm_key, port_no, proxy_shm_id;

  p = getenv (MASTER_SHM_KEY_ENV_STR);
  if (p == NULL)
    {
      UW_SET_ERROR_CODE (UW_ER_SHM_OPEN, 0);
      goto return_error;
    }
  parse_int (&master_shm_key, p, 10);
  SHARD_ERR ("<BROKER> MASTER_SHM_KEY_ENV_STR:[%d:%x]\n", master_shm_key, master_shm_key);

  shm_br = (T_SHM_BROKER *) uw_shm_open (master_shm_key, SHM_BROKER, SHM_MODE_ADMIN);
  if (shm_br == NULL)
    {
      UW_SET_ERROR_CODE (UW_ER_SHM_OPEN, 0);
      goto return_error;
    }

  if ((p = getenv (PORT_NUMBER_ENV_STR)) == NULL)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, 0);
      goto return_error;
    }
  parse_int (&port_no, p, 10);
  for (i = 0, br_index = -1; i < shm_br->num_broker; i++)
    {
      if (shm_br->br_info[i].port == port_no)
	{
	  br_index = i;
	  break;
	}
    }
  if (br_index == -1)
    {
      UW_SET_ERROR_CODE (UW_ER_CANT_CREATE_SOCKET, 0);
      goto return_error;
    }
  br_info_p = &shm_br->br_info[i];

  as_shm_key = br_info_p->appl_server_shm_id;
  SHARD_ERR ("<BROKER> APPL_SERVER_SHM_KEY_STR:[%d:%x]\n", as_shm_key, as_shm_key);

  shm_appl = (T_SHM_APPL_SERVER *) uw_shm_open (as_shm_key, SHM_APPL_SERVER, SHM_MODE_ADMIN);
  if (shm_appl == NULL)
    {
      UW_SET_ERROR_CODE (UW_ER_SHM_OPEN, 0);
      goto return_error;
    }

  return 0;

return_error:
  /* SHARD TODO : NOT IMPLEMENTED YET */
#if 0
  SET_BROKER_ERR_CODE ();
#endif

  /* SHARD TODO : DETACH SHARED MEMORY */

  return -1;
}

static void
get_as_sql_log_filename (char *log_filename, int len, char *broker_name, T_APPL_SERVER_INFO * as_info_p, int as_index)
{
  int ret;
  char dirname[BROKER_PATH_MAX];

  get_cubrid_file (FID_SQL_LOG_DIR, dirname, BROKER_PATH_MAX);

  {
    ret = snprintf (log_filename, BROKER_PATH_MAX - 1, "%s%s_%d.sql.log", dirname, broker_name, as_index + 1);
  }
  if (ret < 0)
    {
      // bad name
      log_filename[0] = '\0';
    }
}

static void
get_as_slow_log_filename (char *log_filename, int len, char *broker_name, T_APPL_SERVER_INFO * as_info_p, int as_index)
{
  int ret;
  char dirname[BROKER_PATH_MAX];

  get_cubrid_file (FID_SLOW_LOG_DIR, dirname, BROKER_PATH_MAX);

  {
    ret = snprintf (log_filename, BROKER_PATH_MAX - 1, "%s%s_%d.slow.log", dirname, broker_name, as_index + 1);
  }
  if (ret < 0)
    {
      // bad name
      log_filename[0] = '\0';
    }
}

static int
read_buffer_async (SOCKET sock_fd, char *buf, int size, int timeout)
{
  int read_len = -1;
  struct pollfd po[1] = { {0, 0, 0} };
  int ret;

  po[0].fd = sock_fd;
  po[0].events = POLLIN;

  do
    {
      ret = poll (po, 1, timeout * 1000);
    }
  while (ret < 0 && errno == EINTR);

  if (ret < 1)			/* ERROR OR TIMEOUT */
    {
      return -1;
    }

  if (po[0].revents & POLLIN)	/* RECEIVE NEW REQUEST */
    {
      read_len = READ_FROM_SOCKET (sock_fd, buf, size);
    }

  return read_len;
}

static int
write_buffer_async (SOCKET sock_fd, char *buf, int size, int timeout)
{
  int write_len = -1;
  struct pollfd po[1] = { {0, 0, 0} };
  int ret;

  po[0].fd = sock_fd;
  po[0].events = POLLOUT;

  do
    {
      ret = poll (po, 1, timeout * 1000);
    }
  while (ret < 0 && errno == EINTR);

  if (ret < 1)			/* ERROR OR TIMEOUT */
    {
      return -1;
    }

  if (po[0].revents & POLLOUT)	/* RECEIVE NEW REQUEST */
    {
      write_len = WRITE_TO_SOCKET (sock_fd, buf, size);
    }

  return write_len;
}
