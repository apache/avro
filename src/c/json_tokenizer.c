#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "json_tokenizer.h"

static struct keyword
{
  char *z;
  int len;
  int tokenType;
} keywords[] =
{
  {
  "true", 4, TK_TRUE},
  {
  "false", 5, TK_FALSE},
  {
  "null", 4, TK_NULL}
};

#define NUM_KEYWORDS (sizeof(keywords)/sizeof(keywords[0]))

int
json_get_token (const char *z, const unsigned len, int *tokenType,
		double *number)
{
  char *p;
  int i;
  if (!z || !tokenType || len == 0 || !number)
    {
      return -1;
    }

  if (isspace (z[0]))
    {
      for (i = 1; isspace (z[i]); i++)
	{
	}
      *tokenType = TK_SPACE;
      return i;
    }

  switch (*z)
    {
    case '"':
      {
	/* Find the end quote */
	for (i = 1; i < len; i++)
	  {
	    /* TODO: escape characters? */
	    if (z[i] == '"' && z[i - 1] != '\\')
	      {
		*tokenType = TK_STRING;
		return i + 1;
	      }
	  }
	/* TODO: think about this... */
	break;
      }
    case ':':
      {
	*tokenType = TK_COLON;
	return 1;
      }
    case ',':
      {
	*tokenType = TK_COMMA;
	return 1;
      }
    case '{':
      {
	*tokenType = TK_LCURLY;
	return 1;
      }
    case '}':
      {
	*tokenType = TK_RCURLY;
	return 1;
      }
    case '[':
      {
	*tokenType = TK_LBRACKET;
	return 1;
      }
    case ']':
      {
	*tokenType = TK_RBRACKET;
	return 1;
      }
    }
  /* check for keywords */
  for (i = 0; i < NUM_KEYWORDS; i++)
    {
      struct keyword *kw = keywords + i;
      if (strncmp ((char *) z, kw->z, kw->len) == 0)
	{
	  *tokenType = kw->tokenType;
	  return kw->len;
	}
    }
  /* Check for number */
  *number = strtod (z, &p);
  if (p != z)
    {
      *tokenType = TK_NUMBER;
      return (p - z);
    }

  /* ???? */
  *tokenType = 0;
  return 1;
}
