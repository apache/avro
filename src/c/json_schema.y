/* 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
%name JSONParser

%token_type {JSON_value *}
%default_type {JSON_value *}

%extra_argument { JSON_ctx *ctx }

%include {
#include <stdio.h>
#include <assert.h>
#include "json.h"
#include "json_tokenizer.h"

#if 0
#define DEBUG_PARSER(stmt) stmt
#else
#define DEBUG_PARSER(stmt)
#endif

#define NUM_INIT_ELEMENTS 16
}

%parse_accept {
   DEBUG_PARSER(fprintf(stderr,"Input parsed and accepted\n"));
}

%syntax_error {
   ctx->error = 1;
   DEBUG_PARSER(fprintf(stderr,"Syntax error\n"));
}

%token_prefix TK_

json ::= jsontext(A).
{
      ctx->result = A;
}

jsontext(A) ::= object(B). { A=B; }
jsontext(A) ::= array(B).  { A=B; }

/* Values */
value(A) ::= STRING(B).  { A=B; }
value(A) ::= NUMBER(B).  { A=B; }
value(A) ::= object(B).  { A=B; }
value(A) ::= array(B).   { A=B; }
value(A) ::= TRUE(B).    { A=B; }
value(A) ::= FALSE(B).   { A=B; }
value(A) ::= NULL(B).    { A=B; }

/* Arrays */
%type element_list {apr_array_header_t *}
%type elements {apr_array_header_t *}

element_list(A) ::= elements(B) COMMA.
{
      A=B;
}
element_list(A) ::= .
{
      A=apr_array_make(ctx->pool, NUM_INIT_ELEMENTS, sizeof(JSON_value *));
}
elements(A) ::= element_list(B) value(C).
{
      A = B;
      *(JSON_value **)apr_array_push(B) = C;
}
elements(A) ::= element_list(B) .
{
      A = B;
}
array(A)  ::= LBRACKET elements(B) RBRACKET.
{
      A = JSON_value_new(ctx->pool, JSON_ARRAY);
      A->array_value = B;
}

/* Objects */
%type member_list {apr_hash_t *}
%type members {apr_hash_t *}
member_list(A) ::= members(B) COMMA.
{
     A = B;
}
member_list(A) ::= .
{
     A = apr_hash_make(ctx->pool);
}
members(A) ::= member_list(B) STRING(C) COLON value(D).
{
     A = B;
     apr_hash_set(B, C->string_value, APR_HASH_KEY_STRING, D);
}
members(A) ::= member_list(B).
{
      A = B;
}
object(A) ::= LCURLY members(B) RCURLY.
{
      A = JSON_value_new(ctx->pool, JSON_OBJECT);
      A->object_value = B;
}
