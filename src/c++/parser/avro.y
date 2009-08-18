%{
/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

#include <stdio.h>
#include <boost/format.hpp>
#include "Compiler.hh"
#include "Exception.hh"

#define YYLEX_PARAM ctx
#define YYPARSE_PARAM ctx

void yyerror(const char *str)
{
    throw avro::Exception(boost::format("Parser error: %1%") % str);
}
 
extern void *lexer; 
extern int yylex(int *, void *);
  
avro::CompilerContext &context(void *ctx) { 
    return *static_cast<avro::CompilerContext *>(ctx);
};
  
%}

%pure-parser
%error-verbose

%token AVRO_LEX_INT AVRO_LEX_LONG AVRO_LEX_FLOAT AVRO_LEX_DOUBLE
%token AVRO_LEX_BOOL AVRO_LEX_NULL AVRO_LEX_BYTES AVRO_LEX_STRING
%token AVRO_LEX_RECORD AVRO_LEX_ENUM AVRO_LEX_ARRAY AVRO_LEX_MAP AVRO_LEX_UNION AVRO_LEX_FIXED
%token AVRO_LEX_SYMBOL AVRO_LEX_SIZE
%token AVRO_LEX_TYPE AVRO_LEX_ITEMS AVRO_LEX_NAME AVRO_LEX_VALUES AVRO_LEX_FIELDS 

%%

avroschema: 
        primitive | avroobject | union_t
        ;

avroobject:
        primitiveobject | record_t | array_t | map_t | enum_t | fixed_t
        ;

primitiveobject:
        '{' AVRO_LEX_TYPE primitive '}'
        ;

primitive:
        AVRO_LEX_INT    { context(ctx).addPrimitive(avro::AVRO_INT); }
        |
        AVRO_LEX_LONG   { context(ctx).addPrimitive(avro::AVRO_LONG); }
        |
        AVRO_LEX_FLOAT  { context(ctx).addPrimitive(avro::AVRO_FLOAT); }
        |
        AVRO_LEX_DOUBLE { context(ctx).addPrimitive(avro::AVRO_DOUBLE); }
        |
        AVRO_LEX_BOOL   { context(ctx).addPrimitive(avro::AVRO_BOOL); }
        |
        AVRO_LEX_NULL   { context(ctx).addPrimitive(avro::AVRO_NULL); }
        |
        AVRO_LEX_BYTES  { context(ctx).addPrimitive(avro::AVRO_BYTES); }
        |
        AVRO_LEX_STRING { context(ctx).addPrimitive(avro::AVRO_STRING); }
        |
        AVRO_LEX_SYMBOL { context(ctx).addSymbol(); }
        ;

recordtag: 
        AVRO_LEX_TYPE AVRO_LEX_RECORD 
        ;

enumtag: 
        AVRO_LEX_TYPE AVRO_LEX_ENUM 
        ;

arraytag:
        AVRO_LEX_TYPE AVRO_LEX_ARRAY
        { context(ctx).addArray(); }
        ;

maptag:
        AVRO_LEX_TYPE AVRO_LEX_MAP
        { context(ctx).addMap(); }
        ;

fixedtag:
        AVRO_LEX_TYPE AVRO_LEX_FIXED
        ;

record_t:
        '{' recordtag ',' name { context(ctx).addRecord() } ',' AVRO_LEX_FIELDS fieldlist '}'
        { context(ctx).endCompound(avro::AVRO_RECORD); }
        ;

enum_t:
       '{'  enumtag ',' name { context(ctx).addEnum() } ',' namelist '}'
        { context(ctx).endCompound(avro::AVRO_ENUM); }
        ;

array_t: 
       '{'  arraytag ',' AVRO_LEX_ITEMS avroschema '}'
        { context(ctx).endCompound(avro::AVRO_ARRAY); }
        ;

map_t: 
        '{' maptag ',' AVRO_LEX_VALUES avroschema '}'
        { context(ctx).endCompound(avro::AVRO_MAP); }
        ;

union_t:
        '[' { context(ctx).addUnion(); } unionlist ']'
        { context(ctx).endCompound(avro::AVRO_UNION); }
        ;

fixed_t:
        '{' fixedtag ',' size ',' name '}'
        { context(ctx).addFixed(); }
        ;

name:
        AVRO_LEX_NAME 
        { context(ctx).addName(); }
        ;

size:
        AVRO_LEX_SIZE 
        { context(ctx).addSize(); }
        ;

namelist:
        name | namelist ',' name
        ;

field:
        '{' fieldname ',' avroschema '}'
        ;   

fieldname:
        AVRO_LEX_NAME 
        { context(ctx).addFieldName(); }
        ;

fieldlist:
        field | fieldlist ',' field
        ;

unionlist: 
        avroschema | unionlist ',' avroschema

