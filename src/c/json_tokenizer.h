#ifndef JSON_TOKENIZER_H
#define JSON_TOKENIZER_H

#include "json_schema.h"

/* Tokens which are not part of the schema */
enum json_tokens
{
  TK_SPACE = 42424242
};

struct Token
{
  char *z;
  double d;
  int b;
};
typedef struct Token Token;

int json_get_token (const char *z, const unsigned len, int *tokenType,
		    double *number);

#endif
