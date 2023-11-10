#ifndef __S62__
#define __S62__

typedef struct s62_metadata {
 char *label_name;
 int type; // 1 : node, 2 : edge
 struct s62_metadata *next;
} S62_METADATA;

typedef struct s62_property {
 char *label_name;
 int  label_type; // 1 : node, 2 : edge, 3 : other
 char *property_name;
 int  order;
 int  type;
 int  sqltype;
 int  precision;
 int  scale;
 struct s62_property *next;
} S62_PROPERTY;

typedef struct s62_statement {
 char *query;
 int  query_type;
 char *plan;
 int  num_property;
 S62_PROPERTY *property;
} S62_STATEMENT;

typedef struct s62_resultset {
 S62_STATEMENT *stmt;
 int position;
 int num_row;
 void *data;
} S62_RESULTSET;

typedef struct dummy_data {
 char data[3][100];
} DUMMY_DATA; 

typedef enum {
    DB_TYPE_FIRST = 0,          /* first for iteration */
    DB_TYPE_UNKNOWN = 0,
    DB_TYPE_NULL = 0,
    DB_TYPE_INTEGER = 1,
    DB_TYPE_FLOAT = 2,
    DB_TYPE_DOUBLE = 3,
    DB_TYPE_STRING = 4,
    DB_TYPE_OBJECT = 5,
    DB_TYPE_SET = 6,
    DB_TYPE_MULTISET = 7,
    DB_TYPE_SEQUENCE = 8,
    DB_TYPE_ELO = 9,            /* obsolete... keep for backward compatibility. maybe we can replace with something else */
    DB_TYPE_TIME = 10,
    DB_TYPE_TIMESTAMP = 11,
    DB_TYPE_DATE = 12,
    DB_TYPE_MONETARY = 13,
    DB_TYPE_VARIABLE = 14,      /* internal use only */
    DB_TYPE_SUB = 15,           /* internal use only */
    DB_TYPE_POINTER = 16,       /* method arguments only */
    DB_TYPE_ERROR = 17,         /* method arguments only */
    DB_TYPE_SHORT = 18,
    DB_TYPE_VOBJ = 19,          /* internal use only */
    DB_TYPE_OID = 20,           /* internal use only */
    DB_TYPE_DB_VALUE = 21,      /* special for esql */
    DB_TYPE_NUMERIC = 22,       /* SQL NUMERIC(p,s) values */
    DB_TYPE_BIT = 23,           /* SQL BIT(n) values */
    DB_TYPE_VARBIT = 24,        /* SQL BIT(n) VARYING values */
    DB_TYPE_CHAR = 25,          /* SQL CHAR(n) values */
    DB_TYPE_NCHAR = 26,         /* SQL NATIONAL CHAR(n) values */
    DB_TYPE_VARNCHAR = 27,      /* SQL NATIONAL CHAR(n) VARYING values */
    DB_TYPE_RESULTSET = 28,     /* internal use only */
    DB_TYPE_MIDXKEY = 29,
    DB_TYPE_TABLE = 30,         /* internal use only */
    DB_TYPE_BIGINT = 31,
    DB_TYPE_DATETIME = 32,
    DB_TYPE_BLOB = 33,
    DB_TYPE_CLOB = 34,
    DB_TYPE_ENUMERATION = 35,
    DB_TYPE_TIMESTAMPTZ = 36,
    DB_TYPE_TIMESTAMPLTZ = 37,
    DB_TYPE_DATETIMETZ = 38,
    DB_TYPE_DATETIMELTZ = 39,
    DB_TYPE_JSON = 40,

    /* aliases */
    DB_TYPE_LIST = DB_TYPE_SEQUENCE,
    DB_TYPE_SMALLINT = DB_TYPE_SHORT,   /* SQL SMALLINT */
    DB_TYPE_VARCHAR = DB_TYPE_STRING,   /* SQL CHAR(n) VARYING values */
    DB_TYPE_UTIME = DB_TYPE_TIMESTAMP,  /* SQL TIMESTAMP */

    DB_TYPE_LAST = DB_TYPE_JSON
  } DB_TYPE;

typedef enum {
  S62_STMT_NONE = -1,
  S62_STMT_ALTER_CLASS,
  S62_STMT_ALTER_SERIAL,
  S62_STMT_COMMIT_WORK,
  S62_STMT_REGISTER_DATABASE,
  S62_STMT_CREATE_CLASS,
  S62_STMT_CREATE_INDEX,
  S62_STMT_CREATE_TRIGGER,
  S62_STMT_CREATE_SERIAL,
  S62_STMT_DROP_DATABASE,
  S62_STMT_DROP_CLASS,
  S62_STMT_DROP_INDEX,
  S62_STMT_DROP_LABEL,
  S62_STMT_DROP_TRIGGER,
  S62_STMT_DROP_SERIAL,
  S62_STMT_EVALUATE,
  S62_STMT_RENAME_CLASS,
  S62_STMT_ROLLBACK_WORK,
  S62_STMT_GRANT,
  S62_STMT_REVOKE,
  S62_STMT_UPDATE_STATS,
  S62_STMT_INSERT,
  S62_STMT_SELECT,
  S62_STMT_UPDATE,
  S62_STMT_DELETE,
  S62_STMT_MERGE,
  S62_STMT_CALL,
  S62_STMT_GET_ISO_LVL,
  S62_STMT_GET_TIMEOUT,
  S62_STMT_GET_OPT_LVL,
  S62_STMT_SET_OPT_LVL,
  S62_STMT_SCOPE,
  S62_STMT_GET_TRIGGER,
  S62_STMT_SET_TRIGGER,
  S62_STMT_SAVEPOINT,
  S62_STMT_PREPARE,
  S62_STMT_ATTACH,
  S62_STMT_USE,
  S62_STMT_REMOVE_TRIGGER,
  S62_STMT_RENAME_TRIGGER,
  S62_STMT_RENAME_SERVER,
  S62_STMT_ALTER_SERVER,
  S62_STMT_ON_LDB,
  S62_STMT_GET_LDB,
  S62_STMT_SET_LDB,
  S62_STMT_GET_STATS,
  S62_STMT_CREATE_USER,
  S62_STMT_DROP_USER,
  S62_STMT_ALTER_USER,
  S62_STMT_SET_SYS_PARAMS,
  S62_STMT_ALTER_INDEX,

  S62_STMT_CREATE_STORED_PROCEDURE,
  S62_STMT_DROP_STORED_PROCEDURE,
  S62_STMT_SELECT_UPDATE,
  S62_STMT_ALTER_STORED_PROCEDURE,
  S62_STMT_ALTER_STORED_PROCEDURE_OWNER = S62_STMT_ALTER_STORED_PROCEDURE,

  S62_STMT_CREATE = S62_STMT_CREATE_CLASS,
  S62_STMT_MATCH = S62_STMT_SELECT,

  S62_MAX_STMT_TYPE
  } S62_STMT_TYPE; 

// connection
extern int s62_connect (char *dbname);
extern void s62_disconnect ();
extern int s62_is_connected ();
extern char *s62_get_version ();

// error
extern int s62_get_lasterror (char *errmsg);

// schema
extern int s62_get_metadata_from_catalog (char *labelname, int like_flag, int filter_flag, S62_METADATA **metadata);
extern void s62_close_metadata (S62_METADATA *metadata);
extern int s62_get_property_from_catalog (char *labelname, int type, S62_PROPERTY **property);
extern void s62_close_property (S62_PROPERTY *property);

// query
extern S62_STATEMENT *s62_prepare (char *query);
extern char *s62_getplan (S62_STATEMENT *statement);
extern int s62_get_property_from_statement (S62_STATEMENT *statement, S62_PROPERTY **property);
extern int s62_execute (S62_STATEMENT *statement, S62_RESULTSET **resultset);
extern int s62_fetch_next (S62_RESULTSET *resultset);
extern int s62_get_property_count (S62_RESULTSET *resultset);
extern int s62_get_property_type (S62_RESULTSET *resultset, int idx);
extern void s62_close_resultset (S62_RESULTSET *resultset);
extern void s62_close_statement (S62_STATEMENT *statement);

// put
extern void s62_bind_string (S62_STATEMENT *statement, int idx, char *value);
extern void s62_bind_short (S62_STATEMENT *statment, int idx, short value);
extern void s62_bind_int (S62_STATEMENT *statment, int idx, int value);

// get
extern char *s62_get_string (S62_RESULTSET *resultset, int idx);
extern short s62_get_short (S62_RESULTSET *resultset, int idx);
extern int s62_get_int (S62_RESULTSET *resultset, int idx);
#endif
