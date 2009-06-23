#include <stdio.h>
#include <stdlib.h>

#include "json.h"
#include "json_tokenizer.h"

static void
ws_depth (FILE * file, int depth)
{
  int i;
  for (i = 0; i < depth; i++)
    {
      fprintf (file, "   ");
    }
}

static void
JSON_print_private (FILE * file, JSON_value * value, int *depth)
{
  int i;
  switch (value->type)
    {
    case JSON_UNKNOWN:
      fprintf (file, "???");
      return;
    case JSON_STRING:
      fprintf (file, "\"%s\"", value->string_value);
      return;
    case JSON_NUMBER:
      fprintf (file, "%E", value->number_value);
      return;
    case JSON_BOOLEAN:
      fprintf (file, "%s", value->boolean_value ? "true" : "false");
      return;
    case JSON_NULL:
      fprintf (file, "null");
      return;
    case JSON_ARRAY:
      (*depth)++;
      fprintf (file, "[\n");
      for (i = 0; i < value->array_value->nelts; i++)
	{
	  if (i)
	    {
	      fprintf (file, ",\n");
	    }
	  ws_depth (file, *depth);
	  JSON_print_private (file,
			      ((JSON_value **) value->array_value->elts)[i],
			      depth);
	}
      fprintf (file, "\n");
      (*depth)--;
      ws_depth (file, *depth);
      fprintf (file, "]");
      break;
    case JSON_OBJECT:
      {
	apr_hash_index_t *hi;
	char *key;
	apr_ssize_t len;
	JSON_value *member_value;

	(*depth)++;
	fprintf (file, "{\n");
	for (i = 0, hi = apr_hash_first (value->pool, value->object_value);
	     hi; hi = apr_hash_next (hi), i++)
	  {
	    if (i)
	      {
		fprintf (file, ",\n");
	      }
	    apr_hash_this (hi, (void *) &key, &len, (void *) &member_value);
	    ws_depth (file, *depth);
	    fprintf (file, "\"%s\" :", key);
	    JSON_print_private (file, member_value, depth);
	  }
	fprintf (file, "\n");
	(*depth)--;
	ws_depth (file, *depth);
	fprintf (file, "}");
      }
      break;
    }
  return;
}

void
JSON_print (FILE * file, JSON_value * value)
{
  int depth = 0;
  if (!file || !value)
    {
      return;
    }
  JSON_print_private (file, value, &depth);
  fprintf (file, "\n");
}

JSON_value *
JSON_value_new (apr_pool_t * pool, int type)
{
  JSON_value *value = NULL;
  if (pool)
    {
      value = (JSON_value *) apr_palloc (pool, sizeof (JSON_value));
      value->pool = pool;
      value->type = type;
      /* TODO: mark callbacks based on type */
    }
  return value;
}

static JSON_value *
JSON_parse_inner (void *jsonp, apr_pool_t * pool, char *text, int text_len)
{
  int len;
  char *cur, *text_end;
  JSON_value *value = NULL;
  JSON_ctx ctx;

  /* Setup the context */
  ctx.pool = pool;
  ctx.error = 0;
  ctx.result = NULL;

  /* Loop through the input */
  for (cur = text, text_end = text + text_len; cur < text_end; cur += len)
    {
      int tokenType;
      double number;

      len = json_get_token (cur, text_end - cur, &tokenType, &number);
      if (len < 0)
	{
	  return NULL;
	}

      value = NULL;
      switch (tokenType)
	{
	  /* Manage our terminals here.  Non-terminals are managed in the schema. */

	case TK_SPACE:
	  /* Ignore whitespace */
	  continue;

	case TK_COLON:
	case TK_COMMA:
	  /* Don't create JSON_values for these terminals */
	  break;

	case TK_STRING:
	  value = JSON_value_new (pool, JSON_STRING);
	  value->string_value = apr_palloc (pool, len + 1);
	  /* Take off the quotes */
	  memcpy (value->string_value, cur + 1, len - 1);
	  /* TODO: e.g. substitute \" for " */
	  value->string_value[len - 2] = '\0';
	  break;

	case TK_NUMBER:
	  value = JSON_value_new (pool, JSON_NUMBER);
	  value->number_value = number;
	  break;

	case TK_TRUE:
	case TK_FALSE:
	  value = JSON_value_new (pool, JSON_BOOLEAN);
	  value->boolean_value = tokenType == TK_FALSE ? 0 : 1;
	  break;

	case TK_NULL:
	  value = JSON_value_new (pool, JSON_NULL);
	  break;

	}

      JSONParser (jsonp, tokenType, value, &ctx);
      if (ctx.error)
	{
	  return NULL;
	}
    }
  JSONParser (jsonp, 0, value, &ctx);
  return ctx.result;
}

JSON_value *
JSON_parse (apr_pool_t * pool, char *text, int text_len)
{
  JSON_value *value;
  /* Too bad I can't use the pool here... */
  void *jsonp = JSONParserAlloc (malloc);
  if (jsonp == NULL)
    {
      return NULL;
    }
  value = JSON_parse_inner (jsonp, pool, text, text_len);
  JSONParserFree (jsonp, free);
  return value;
}
