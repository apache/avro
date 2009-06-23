#ifndef JSON_H
#define JSON_H

#include <apr.h>
#include <apr_pools.h>
#include <apr_tables.h>
#include <apr_hash.h>

enum JSON_type
{
  JSON_UNKNOWN,
  JSON_OBJECT,
  JSON_ARRAY,
  JSON_STRING,
  JSON_NUMBER,
  JSON_BOOLEAN,
  JSON_NULL
};
typedef enum JSON_type JSON_type;

struct JSON_value
{
  JSON_type type;
  union
  {
    apr_hash_t *object;
    apr_array_header_t *array;
    char *z;
    double number;
    int boolean;
  } value_u;
  apr_pool_t *pool;
};
typedef struct JSON_value JSON_value;
#define object_value value_u.object
#define array_value value_u.array
#define string_value value_u.z
#define number_value value_u.number
#define boolean_value value_u.boolean

JSON_value *JSON_parse (apr_pool_t * pool, char *text, int text_len);

JSON_value *JSON_value_new (apr_pool_t * pool, int type);

void JSON_print (FILE * file, JSON_value * value);

struct JSON_ctx
{
  apr_pool_t *pool;
  JSON_value *result;
  int error;
};
typedef struct JSON_ctx JSON_ctx;

/* in json_schema.c */
void *JSONParserAlloc (void *(*mallocProc) (size_t));
void JSONParser (void *yyp, int yymajor, JSON_value * value, JSON_ctx * ctx);
void JSONParserFree (void *p, void (*freeProc) (void *));
void JSONParserTrace (FILE * TraceFILE, char *zTracePrompt);

#endif
