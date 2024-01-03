#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <string.h>
#include "environment_variable.h"

#include "s62ext.h"

static char _s62_version[20] = { "11.3.0.0001" };

s62_version s62_get_version_ex()
{
  return (_s62_version);
}

// get workspace from databases.txt
int s62_get_workspace (const char *dbname, char *workspace)
{
  const char *database;
  char filename[500];
  char buf[500];
  char *ptr;
  FILE *fd;
  bool is_find = false;

  database = envvar_get ((const char *)"DATABASES");

  if (database == NULL) 
    {
      return -1; // can't get enviornment
    }

  sprintf (filename, "%s/databases.txt", database);

  fd = fopen (filename, "r");
  if (fd == NULL)
    {
      return -2; // can't open databases.txt
    }
  
  while (!feof(fd))
    {
	fgets(buf, sizeof(buf), fd);
	if (buf[0] == '#') continue; // check comment

	ptr = strtok (buf, " \t");
	if (ptr == NULL || strcmp (dbname, ptr) != 0) continue;

	ptr = strtok (NULL, " \t");
	if (ptr == NULL) continue;

	strcpy(workspace, ptr);
	is_find = true;
    }

  fclose(fd);

  if (is_find) 
    {
       for (ptr = workspace; *ptr != '\0'; ptr++)
         {
            if (*ptr == '\n' || *ptr == '\t' || *ptr == ' ')
              {
                 *ptr = '\0';
              }
         }
       return (0);
    }
  else
    {
       return (-3); // can't find workspace with dbname in databases.txt
    }
}

char *
s62_getplan (S62_STATEMENT * statement)
{
  if (statement != NULL)
    {
      return statement->plan;
    }

  return NULL;
}

int
s62_get_property_from_statement (S62_STATEMENT * statement, S62_PROPERTY ** property)
{
  int num_properties = 0;

  if (statement != NULL)
    {
      *property = statement->property;
      num_properties = statement->num_properties;
    }
  else
    {
      *property = NULL;
    }

  return (num_properties);
}

int
s62_get_property_count (S62_RESULTSET * resultset)
{
  int num_properties = 0;

  if (resultset != NULL && resultset->result_set != NULL)
    {
      num_properties = resultset->result_set->num_properties;
    }
  else
    {
      num_properties = 0;
    }

  return num_properties;
}

s62_type
s62_get_property_type (S62_STATEMENT * statement, int idx)
{
  S62_PROPERTY *ptr = NULL;
  s62_type type;
  int i;

  if (statement != NULL && statement->property != NULL)
    {
      i = 0;
      for (ptr = statement->property; ptr != NULL; ptr = ptr->next, i++)
	{
	  if (i == idx)
	    {
	      type = ptr->property_type;
	      break;
	    }
	}

      if (i != idx)
	{
	  type = S62_TYPE_INVALID;
	}
    }
  else
    {
      type = S62_TYPE_INVALID;
    }

  return type;
}


void
s62_close_statement (S62_STATEMENT * statement)
{
  if (statement == NULL)
    {
      return;
    }

#if 0
  if (statement->query != NULL)
    free (statement->query);
  if (statement->plan != NULL)
    free (statement->plan);
  s62_close_property(statement->property);
#endif

  free (statement);

  statement = NULL;
}

DB_TYPE s62type_to_dbtype (s62_type type)
{
   DB_TYPE db_type = DB_TYPE_UNKNOWN;

   switch (type)
     {
	case S62_TYPE_SMALLINT :
	case S62_TYPE_USMALLINT :
	  db_type = DB_TYPE_SHORT;
	  break;
	case S62_TYPE_INTEGER :
	case S62_TYPE_UINTEGER :
	  db_type = DB_TYPE_INTEGER;
	  break;
	case S62_TYPE_BIGINT :
	case S62_TYPE_UBIGINT :
	case S62_TYPE_ID :
	  db_type = DB_TYPE_BIGINT;
	  break;
	case S62_TYPE_FLOAT :
	  db_type = DB_TYPE_FLOAT;
	  break;
	case S62_TYPE_DOUBLE :
	  db_type = DB_TYPE_DOUBLE;
	  break;
	case S62_TYPE_DECIMAL :
	  db_type = DB_TYPE_NUMERIC;
	  break;
	case S62_TYPE_VARCHAR :
	  db_type = DB_TYPE_STRING;
	  break;
	case S62_TYPE_TIME :
	  db_type = DB_TYPE_TIME;
	  break;
	case S62_TYPE_TIMESTAMP :
	  db_type = DB_TYPE_TIMESTAMP;
	  break;
	case S62_TYPE_DATE :
	  db_type = DB_TYPE_DATE;
	  break;
	default :
	  db_type = DB_TYPE_UNKNOWN;
	  break;
     }

   return (db_type);
}

s62_type dbtype_to_s62type (DB_TYPE type)
{
   s62_type s62_type = S62_TYPE_INVALID;

   switch (type)
     {
	case DB_TYPE_SHORT :
	  s62_type = S62_TYPE_SMALLINT;
	  break;
	case DB_TYPE_INTEGER :
	  s62_type = S62_TYPE_INTEGER;
	  break;
	case DB_TYPE_BIGINT :
	  s62_type = S62_TYPE_BIGINT;
	  break;
	case DB_TYPE_FLOAT :
	  s62_type = S62_TYPE_FLOAT;
	  break;
	case DB_TYPE_DOUBLE :
	  s62_type = S62_TYPE_DOUBLE;
	  break;
	case DB_TYPE_NUMERIC :
	  s62_type = S62_TYPE_DECIMAL;
	  break;
	case DB_TYPE_STRING :
	  s62_type = S62_TYPE_VARCHAR;
	  break;
	case DB_TYPE_TIME :
	  s62_type = S62_TYPE_TIME;
	  break;
	case DB_TYPE_TIMESTAMP :
	case DB_TYPE_DATETIME :
	  s62_type = S62_TYPE_TIMESTAMP;
	  break;
	case DB_TYPE_DATE :
	  s62_type = S62_TYPE_DATE;
	  break;
	default :	
	  s62_type = S62_TYPE_INVALID;
	  break;
     }

   return (s62_type);
}
