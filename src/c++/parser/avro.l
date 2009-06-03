%{
#include "avro.tab.h"

// this undef is a hack for my mac implementation
#undef yyFlexLexer
#include "Compiler.hh"

#define YY_STACK_USED 1

using std::cin;
using std::cout;
using std::cerr;

%}

%option c++
%option noyywrap

%{

int yylex(int *val, void *ctx)
{
    avro::CompilerContext *c = static_cast<avro::CompilerContext *>(ctx);
    int ret = c->lexer().yylex();
    if(ret == AVRO_LEX_NAME || ret == AVRO_LEX_SYMBOL || ret == AVRO_LEX_SIZE) {
        c->setText( c->lexer().YYText()) ;
    }
    return ret;
}

%}

%x READTYPE
%x STARTTYPE
%x READNAME
%x READFIELDS
%x READSYMBOLS
%s READSIZE
%x INUNION
%x INOBJECT

ws [ \t\n]
delim {ws}*:{ws}*
avrotext [a-zA-Z_][a-zA-Z0-9_]*
startunion \[
startobject \{
integer [0-9]+

%%
<READTYPE>int                  return AVRO_LEX_INT;
<READTYPE>long                 return AVRO_LEX_LONG;
<READTYPE>null                 return AVRO_LEX_NULL;
<READTYPE>boolean              return AVRO_LEX_BOOL;
<READTYPE>float                return AVRO_LEX_FLOAT;
<READTYPE>double               return AVRO_LEX_DOUBLE;
<READTYPE>string               return AVRO_LEX_STRING;
<READTYPE>bytes                return AVRO_LEX_BYTES;
<READTYPE>record               return AVRO_LEX_RECORD;
<READTYPE>enum                 return AVRO_LEX_ENUM;
<READTYPE>map                  return AVRO_LEX_MAP;
<READTYPE>array                return AVRO_LEX_ARRAY;
<READTYPE>fixed                return AVRO_LEX_FIXED;
<READTYPE>{avrotext}           return AVRO_LEX_SYMBOL;
<READTYPE>\"                   yy_pop_state(); 


<READNAME>{avrotext}           return AVRO_LEX_NAME;
<READNAME>\"                   yy_pop_state();

<READFIELDS>\"type\"{delim}    yy_push_state(STARTTYPE); 
<READFIELDS>\"name\"{delim}\"  yy_push_state( READNAME); 
<READFIELDS>\]                 yy_pop_state(); 
<READFIELDS>[,\{\}]            return yytext[0];
<READFIELDS>{ws}               ;

<READSYMBOLS>\"                 yy_push_state(READNAME); 
<READSYMBOLS>,                  return yytext[0];
<READSYMBOLS>\]                 yy_pop_state();
<READSYMBOLS>{ws}               ;

<READSIZE>{integer}             yy_pop_state(); return AVRO_LEX_SIZE;

<INUNION>\"                 yy_push_state(READTYPE); 
<INUNION>{startobject}      yy_push_state( INOBJECT); return yytext[0];
<INUNION>\]                 yy_pop_state(); return yytext[0];
<INUNION>,                  return yytext[0];
<INUNION>{ws}               ;

<INOBJECT>\"type\"{delim}      yy_push_state(STARTTYPE); return AVRO_LEX_TYPE;
<INOBJECT>\"name\"{delim}\"    yy_push_state(READNAME); 
<INOBJECT>\"size\"{delim}      yy_push_state(READSIZE);
<INOBJECT>\"items\"{delim}     yy_push_state(STARTTYPE); return AVRO_LEX_ITEMS;
<INOBJECT>\"values\"{delim}    yy_push_state(STARTTYPE); return AVRO_LEX_VALUES;
<INOBJECT>\"fields\"{delim}\[  yy_push_state( READFIELDS); return AVRO_LEX_FIELDS; 
<INOBJECT>\"symbols\"{delim}\[ yy_push_state( READSYMBOLS); 
<INOBJECT>,                    return yytext[0];
<INOBJECT>\}                   yy_pop_state(); return yytext[0];
<INOBJECT>{ws}                 ;

<STARTTYPE>\"                  yy_pop_state(); yy_push_state(READTYPE); 
<STARTTYPE>{startunion}        yy_pop_state(); yy_push_state(INUNION); return yytext[0];
<STARTTYPE>{startobject}       yy_pop_state(); yy_push_state(INOBJECT); return yytext[0];

{startobject}                  yy_push_state( INOBJECT); return yytext[0];
{startunion}                   yy_push_state( INUNION); return yytext[0];
\"                             yy_push_state( READTYPE);
{ws}                           ;

%%

