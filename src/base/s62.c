#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <string.h>

#include "s62.h"

#if 0
// connection
int s62_connect (char *dbname);
void s62_disconnect ();
int s62_is_connected ();
char *s62_get_version ();

// error
int s62_get_lasterror (char *errmsg);

// schema
int s62_get_metadata_from_catalog (char *labelname, int like_flag, int filter_flag, S62_METADATA **metdata);
void s62_close_metadata (S62_METADATA *metadata);
int s62_get_property_from_catalog (char *labelname, int type, S62_PROPERTY **property);
void s62_close_property (S62_PROPERTY *property);

// query
S62_STATEMENT *s62_prepare (char *query);
char *s62_getplan (S62_STATEMENT *statement);
int s62_get_property_from_statement (S62_STATEMENT *statement, S62_PROPERTY **property);
int s62_execute (S62_STATEMENT *statement, S62_RESULTSET **resultset);
int s62_fetch_next (S62_RESULTSET *resultset);
int s62_get_property_count (S62_RESULTSET *resultset);
int s62_get_property_type (S62_RESULTSET *resultset, int idx);
void s62_close_resultset (S62_RESULTSET *resultset);
void s62_close_statement (S62_STATEMENT *statement);

// put
void s62_bind_string (S62_STATEMENT *statement, int idx, char *value);
void s62_bind_byte (S62_STAEMENT *statment, int idx, char value);
void s62_bind_short (S62_STAEMENT *statment, int idx, short value);
void s62_bind_int (S62_STAEMENT *statment, int idx, int value);

// get
char *s62_get_string (S62_RESULTSET *resultset, int idx);
char s62_get_byte (S62_RESULTSET *resultset, int idx);
short s62_get_short (S62_RESULTSET *resultset, int idx);
int s62_get_int (S62_RESULTSET *resultset, int idx);
#endif

static S62_METADATA *add_meta (S62_METADATA *meta_res, char *label_name, int label_type);
static void release_meta_results (S62_METADATA *meta_res);
static S62_PROPERTY *add_property (S62_PROPERTY *property_res, char *lable_name, int label_type, char *property_name, int order, int type, int sqltype, int precision, int scale);
static void release_property_results (S62_PROPERTY *property_res);

bool is_connected = false;
static char s62_version[20] = {"11.3.0.0001"};;

int s62_connect (char *dbname)
{
 is_connected = true;
 return 0;
}

void s62_disconnect ()
{
 is_connected = false;
}

int s62_is_connected ()
{
 return (is_connected);
}

char *s62_get_version ()
{
 return (s62_version);
}

// error
int s62_get_lasterror (char *errmsg)
{
int err_code = -1;

 if (errmsg != NULL)
   {
     sprintf(errmsg, "error from s62");
   }

 return (err_code);
}

// schema
static S62_METADATA *add_meta (S62_METADATA *meta_res, char *label_name, int label_type)
{
S62_METADATA *ptr;

 ptr = (S62_METADATA *) malloc (sizeof(S62_METADATA));
 if (ptr == NULL)
   {
      release_meta_results (meta_res);
      return NULL;
   }
 memset (ptr, 0x00, sizeof(S62_METADATA));

 ptr->label_name = strdup(label_name);
 ptr->type = label_type;

 if (meta_res != NULL)
   {
     ptr->next = meta_res;
   }

 return ptr;
}

static void release_meta_results (S62_METADATA *meta_res)
{
S62_METADATA *ptr, *tmp;

 if (meta_res == NULL) return;

 for (ptr = meta_res; ptr != NULL; )
   {
      if (ptr->label_name != NULL) free (ptr->label_name);
      tmp = ptr;
      ptr = ptr->next;
      free (tmp);
   }

 meta_res = (S62_METADATA *) NULL;
}

static S62_PROPERTY *add_property (S62_PROPERTY *property_res, char *lable_name, int label_type, char *property_name, int order, int type, int sqltype, int precision, int scale)
{
S62_PROPERTY *ptr;

 ptr = (S62_PROPERTY *) malloc(sizeof(S62_PROPERTY));
 if (ptr == NULL)
  {
    release_property_results (property_res);
    return NULL;
  }
 memset (ptr, 0x00, sizeof(S62_PROPERTY));

 ptr->label_name = strdup(lable_name);
 ptr->label_type = label_type;
 ptr->property_name = strdup(property_name);
 ptr->order = order;
 ptr->type = type;
 ptr->sqltype = sqltype;
 ptr->precision = precision;
 ptr->scale = scale;

 if (property_res != NULL)
   {
     ptr->next = property_res;
   }

 return ptr;
}

static void release_property_results (S62_PROPERTY *property_res)
{
S62_PROPERTY *ptr, *tmp;

 if (property_res == NULL) return;

 for (ptr = property_res; ptr != NULL; )
   {
      if (ptr->label_name != NULL) free (ptr->label_name);
      if (ptr->property_name != NULL) free (ptr->property_name);
      tmp = ptr;
      ptr = ptr->next;
      free (tmp);
   }

 property_res = (S62_PROPERTY *) NULL;
}

int s62_get_metadata_from_catalog (char *labelname, int like_flag, int filter_flag, S62_METADATA **metadata)
{
S62_METADATA *meta_res;
int num_results = 0;

 meta_res = (S62_METADATA *) NULL;
 meta_res = add_meta (meta_res, (char *)"node_sample", 1);
 if (meta_res != NULL) num_results++;

 meta_res = add_meta (meta_res, (char *)"edge_sample", 2);
 if (meta_res != NULL) num_results++;

 *metadata = meta_res;

 return num_results; 
}

void s62_close_metadata (S62_METADATA *metadata)
{
 release_meta_results (metadata);
}

int s62_get_property_from_catalog (char *labelname, int type, S62_PROPERTY **property)
{
S62_PROPERTY *property_res;
int num_results = 0;

 property_res = (S62_PROPERTY *) NULL;

 if (strcmp(labelname, "node_sample") == 0)
   {
     property_res = add_property(property_res, labelname, 1, (char *)"node_property1", 1, DB_TYPE_STRING, DB_TYPE_STRING, 20, 0);
     if (property_res != NULL) num_results++;
     property_res = add_property(property_res, labelname, 1, (char *)"node_property2", 2, DB_TYPE_INTEGER, DB_TYPE_INTEGER, 8, 0);
     if (property_res != NULL) num_results++;
   }
 else if (strcmp(labelname, "edge_sample") == 0)
   {
     property_res = add_property(property_res, labelname, 2, (char *)"edge_property1", 1, DB_TYPE_STRING, DB_TYPE_STRING, 20, 0);
     if (property_res != NULL) num_results++;
     property_res = add_property(property_res, labelname, 2, (char *)"edge_property2", 2, DB_TYPE_INTEGER, DB_TYPE_INTEGER, 8, 0);
     if (property_res != NULL) num_results++;
   }

 *property = property_res;

 return num_results;
}

void s62_close_property (S62_PROPERTY *property)
{
 release_property_results (property);
}

// query
S62_STATEMENT *s62_prepare (char *query)
{
S62_STATEMENT *stmt = (S62_STATEMENT *) NULL;

  stmt = (S62_STATEMENT *) malloc (sizeof(S62_STATEMENT));
  memset(stmt, 0x00, sizeof(S62_STATEMENT));

  if (stmt != NULL)
    {
       stmt->query = strdup(query);
       stmt->query_type = S62_STMT_MATCH;
       stmt->plan = NULL;

       stmt->property = add_property(stmt->property, (char *)"node_sample", 1, (char *)"node_property", 1, DB_TYPE_STRING, DB_TYPE_STRING, 20, 0);
       if (stmt->property != NULL) stmt->num_property++;
       stmt->property = add_property(stmt->property, (char *)"edge_sample", 2, (char *)"edge_property", 2, DB_TYPE_STRING, DB_TYPE_STRING, 20, 0);
       if (stmt->property != NULL) stmt->num_property++;
       stmt->property = add_property(stmt->property, (char *)"", 0, (char *)"node+edge", 3, DB_TYPE_STRING, DB_TYPE_STRING, 40, 0);
       if (stmt->property != NULL) stmt->num_property++;
    }

  return stmt;
}

char *s62_getplan (S62_STATEMENT *statement)
{
  if (statement != NULL) 
    {
       return statement->plan;
    }

  return NULL;
}

int s62_get_property_from_statement (S62_STATEMENT *statement, S62_PROPERTY **property)
{
int num_property = 0;

  if (statement != NULL)
    {
       *property = statement->property;
       num_property = statement->num_property;
    }
  else 
    {
       *property = NULL;
    }

  return (num_property);
}

int s62_execute (S62_STATEMENT *statement, S62_RESULTSET **resultset)
{
S62_RESULTSET *result = (S62_RESULTSET *) NULL;
int num_result = 0;
int i, j;
DUMMY_DATA *ptr;

 *resultset = NULL; 
 result = (S62_RESULTSET *) malloc (sizeof (S62_RESULTSET));
 if (result == NULL) 
   {
     return (-1); 
   }
 result->stmt = statement;
 result->position = 0;
 result->num_row = 2;
 ptr = (DUMMY_DATA *) malloc (sizeof(DUMMY_DATA) * result->num_row);
 if (result->data == NULL)
   {
     free (result);
     result = NULL; 
     return (-2);
   }
 result->data = (void *)ptr;

 for (i = 0; i < result->num_row; i++)
   {
     for (j = 0; j < 3; j++)
       {
	 memset (ptr->data[j], 0x00, sizeof(100));
     	 sprintf (ptr->data[j], "%d-th data-%d", i, j);
       }
     ptr++;
   }

 *resultset = result; 
 num_result = result->num_row;

 return (num_result);
}

int s62_fetch_next (S62_RESULTSET *resultset)
{
 if (resultset == NULL || resultset->position >= resultset->num_row)
   {
      return (-1);
   }

 resultset->position++;

 if (resultset->position < resultset->num_row)
   {
      return (1);
   }
 else
   {
      return (0);
   }
}

int s62_get_property_count (S62_RESULTSET *resultset)
{
int num_property = 0;

 if (resultset != NULL && resultset->stmt != NULL)
   {
      num_property = resultset->stmt->num_property;
   }
 else
   {
      num_property = 0;
   }

  return num_property;
}

int s62_get_property_type (S62_RESULTSET *resultset, int idx)
{
S62_PROPERTY *ptr = NULL;
int type;
int i;

 if (resultset != NULL && resultset->stmt != NULL && resultset->stmt->property != NULL)
   {
     i = 0;
     for (ptr = resultset->stmt->property; ptr != NULL; ptr = ptr->next, i++)
       {
	  if (i == idx) 
            {
              type = ptr->type;
	      break;
            }
       }

     if (i != idx)
       {
	  type = DB_TYPE_NULL;
       }
   }
 else
   {
      type = DB_TYPE_NULL;
   }

 return type;
}

void s62_close_resultset (S62_RESULTSET *resultset)
{
 if (resultset == NULL)
   {
     return;
   }

 if (resultset->data != NULL) free (resultset->data);

 free (resultset);

 resultset = NULL;
}

void s62_close_statement (S62_STATEMENT *statement)
{
 if (statement == NULL)
   {
      return;
   }

 if (statement->query != NULL) free (statement->query);
 if (statement->plan != NULL) free (statement->plan);
 release_property_results (statement->property);

 free(statement);

 statement = NULL;
}

// put
void s62_bind_string (S62_STATEMENT *statement, int idx, char *value)
{
}

void s62_bind_short (S62_STATEMENT *statment, int idx, short value)
{
}

void s62_bind_int (S62_STATEMENT *statment, int idx, int value)
{
}

// get
char *s62_get_string (S62_RESULTSET *resultset, int idx)
{
 DUMMY_DATA *ptr;
 static char data[100];

 if (resultset == NULL 
     || resultset->position <= 0 || resultset->position > resultset->num_row
     || idx < 0 || idx > 2)
   {
      return NULL;
   }

 ptr = (DUMMY_DATA *) resultset->data + (resultset->position - 1);

 sprintf (data, "%s", ptr->data[idx]);

 return (data);
}

short s62_get_short (S62_RESULTSET *resultset, int idx)
{
}

int s62_get_int (S62_RESULTSET *resultset, int idx)
{
}
