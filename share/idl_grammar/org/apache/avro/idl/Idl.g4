/*
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
grammar Idl;

/*
** This file is a combined grammar to recognize the Avro IDL "language". By
** design, there are no actions in this grammar: this ensures the grammar is
** usable for any language supported by ANTLR.
**
** Some names, like BTrue & BFalse for booleans, may look a bit strange, but
** that's because they can otherwise conflict with identifiers in target\
** languages like Java, Python, etc.
**
** Implementers can implement a listener or visitor to turn a parse result into
** a protocol and/or schema.
*/


/**************************************
**                                   **
**  Parser                           **
**                                   **
**************************************/

// \u001a is the ascii 'sub'(stitute) character, used as end-of-file marker in older systems. It was also used as "end of character stream".
// Thus, accept it at end of the input and ignore anything that comes after it. (See: https://en.wikipedia.org/wiki/Substitute_character)
idlFile: (
    protocol=protocolDeclaration |
    namespace=namespaceDeclaration? mainSchema=mainSchemaDeclaration? (imports+=importStatement|namedSchemas+=namedSchemaDeclaration)*
) ('\u001a' .*?)? EOF;

protocolDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* Protocol name=identifier body=protocolDeclarationBody;

protocolDeclarationBody : LBrace (imports+=importStatement|namedSchemas+=namedSchemaDeclaration|messages+=messageDeclaration)* RBrace ;

namespaceDeclaration: Namespace namespace=identifier Semicolon;

mainSchemaDeclaration: Schema mainSchema=fullType Semicolon;

/**
 * The parser accepts anything that's not a symbol as an identifier. That is, it accepts both an IdentifierToken and all keywords. Which
 * identifiers are actually allowed is context dependent and decided when transforming the parse tree.
 */
identifier: word=(IdentifierToken
  | Protocol
  | Namespace
  | Import
  | IDL
  | Schema
  | Enum
  | Fixed
  | Error
  | Record
  | Array
  | Map
  | Union
  | Boolean
  | Int
  | Long
  | Float
  | Double
  | String
  | Bytes
  | Null
  | BTrue
  | BFalse
  | Decimal
  | Date
  | Time
  | Timestamp
  | LocalTimestamp
  | UUID
  | Void
  | Oneway
  | Throws
);

schemaProperty: At name=identifier LParen value=jsonValue RParen;

importStatement: Import importType=(Schema|Protocol|IDL) location=StringLiteral Semicolon;

namedSchemaDeclaration: fixedDeclaration | enumDeclaration | recordDeclaration;

fixedDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* Fixed name=identifier LParen size=IntegerLiteral RParen Semicolon;

enumDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* Enum name=identifier
    LBrace (enumSymbols+=enumSymbol (Comma enumSymbols+=enumSymbol)*)? RBrace defaultSymbol=enumDefault?;

enumSymbol: (doc=DocComment)? schemaProperties+=schemaProperty* name=identifier;

enumDefault : Equals defaultSymbolName=identifier Semicolon;

recordDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* recordType=(Record|Error) name=identifier body=recordBody;

recordBody : LBrace fields+=fieldDeclaration* RBrace;

fieldDeclaration: (doc=DocComment)? fieldType=fullType variableDeclarations+=variableDeclaration (Comma variableDeclarations+=variableDeclaration)* Semicolon;

variableDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* fieldName=identifier (Equals defaultValue=jsonValue)?;

messageDeclaration: (doc=DocComment)? schemaProperties+=schemaProperty* returnType=resultType name=identifier
    LParen (formalParameters+=formalParameter (Comma formalParameters+=formalParameter)*)? RParen
    (oneway=Oneway | Throws errors+=identifier (Comma errors+=identifier)*)? Semicolon;

formalParameter: (doc=DocComment)? parameterType=fullType parameter=variableDeclaration;

resultType: Void | plainType;

fullType: schemaProperties+=schemaProperty* plainType;

plainType: arrayType | mapType | unionType | nullableType;

// identifier MUST be last, as it captures any other type as well.
nullableType: (primitiveType | referenceName=identifier) optional=QuestionMark?;

primitiveType: typeName=(Boolean | Int | Long | Float | Double | Bytes | String | Null | Date | Time | Timestamp | LocalTimestamp | UUID)
    | typeName=Decimal LParen precision=IntegerLiteral ( Comma scale=IntegerLiteral )? RParen;

arrayType: Array LT elementType=fullType GT;

mapType: Map LT valueType=fullType GT;

unionType: Union LBrace types+=fullType (Comma types+=fullType)* RBrace;

jsonValue: jsonObject | jsonArray | jsonLiteral;
jsonLiteral: literal=(StringLiteral | IntegerLiteral | FloatingPointLiteral | BTrue | BFalse | Null);
jsonObject: LBrace jsonPairs+=jsonPair (Comma jsonPairs+=jsonPair)* RBrace;
jsonPair: name=StringLiteral Colon value=jsonValue;
jsonArray: LBracket (jsonValues+=jsonValue (Comma jsonValues+=jsonValue)*)? RBracket;


/**************************************
**                                   **
**  Lexer                            **
**                                   **
**************************************/

/*
** Comments
*/

// Note 1: this might be more efficient using lexer modes, but these cannot be used in a mixed file like this.
// Note 1: To do so, split this file into 'idl_lexer.g4' and 'idl_parser.g4', and import the tokens with 'options { tokenVocab=idl_lexer; }'

// Note 2: DOC_COMMENT is now a regular token.
DocComment: '/**' .*? '*/' -> channel(HIDDEN);
EmptyComment: '/**/' -> skip;
MultiLineComment: '/*' ~[*] .*? '*/' -> skip;
SingleLineComment: '//' .*? ('\n' | '\r' '\n'?) -> skip;

/*
** Whitespace
*/
// Should be after the rule(s) for single-line comments, especially if rewritten to use multiple lexer modes
WS: [ \t\n\r\f] -> skip;

/*
** Simple tokens
*/
Protocol: 'protocol';
Namespace: 'namespace';
Import: 'import';
IDL: 'idl';
Schema: 'schema';
Enum: 'enum';
Fixed: 'fixed';
Error: 'error';
Record: 'record';
Array: 'array';
Map: 'map';
Union: 'union';
Boolean: 'boolean';
Int: 'int';
Long: 'long';
Float: 'float';
Double: 'double';
String: 'string';
Bytes: 'bytes';
Null: 'null';
// The boolean values are not named True/False to avoid name conflicts with e.g. Python
BTrue: 'true';
BFalse: 'false';
Decimal: 'decimal';
Date: 'date';
Time: 'time_ms';
Timestamp: 'timestamp_ms';
LocalTimestamp: 'local_timestamp_ms';
UUID: 'uuid';
Void: 'void';
Oneway: 'oneway';
Throws: 'throws';
LParen: '(';
RParen: ')';
LBrace: '{';
RBrace: '}';
LBracket: '[';
RBracket: ']';
Colon: ':';
Semicolon: ';';
Comma: ',';
At: '@';
Equals: '=';
Dot: '.';
Dash: '-';
QuestionMark: '?';
LT: '<';
GT: '>';

/*
** Complex tokens
*/

// TODO: restrict to JSON string & number literals?

// We use a reluctant qualifier, so we don't need to forbid the closing quote
StringLiteral: '"' (~[\\\n\r\b\f\t] | '\\' ([nrbft\\'"] | OctDigit OctDigit? | [0-3] OctDigit OctDigit | 'u' HexDigit HexDigit HexDigit HexDigit))*? '"';
//StringLiteral: '"' (~[\\\u0000-\u001F] | '\\' ["\\/nrbft])*? '"';

IntegerLiteral: '-'? ( DecimalLiteral | HexLiteral | OctalLiteral ) [lL]?;
fragment DecimalLiteral: Digit9 Digit*;
fragment HexLiteral: '0' [xX] HexDigit+;
fragment OctalLiteral: '0' OctDigit*;

fragment Digit9: [1-9];
fragment Digit: '0' | Digit9;
fragment HexDigit: [0-9a-fA-F];
fragment OctDigit: [0-7];

FloatingPointLiteral: [+-]? ('NaN' | 'Infinity' | DecimalFloatingPointLiteral | HexadecimalFloatingPointLiteral );
fragment DecimalFloatingPointLiteral: (Digit+ '.' Digit* | '.' Digit+) DecimalExponent? [fFdD]? | Digit+ (DecimalExponent [fFdD]? | [fFdD]);
fragment DecimalExponent: [eE] [+\-]? Digit+;
fragment HexadecimalFloatingPointLiteral: '0' [xX] ( HexDigit+ ('.')? | HexDigit* '.' HexDigit+ ) HexadecimalExponent [fFdD]?;
fragment HexadecimalExponent: [pP] [+\-]? Digit+;

/**
 * An identifier token accepts any sequence of unicoode identifiers, optionally surrounded by backticks, separated by dots and/or dashes.
 * Note that any sequence of identifier parts is an identifier token, even if an identifier part (also) matches an existing keyword.
 * Also note that this token should *only* be used in the identifier grammar rule above.
 */
IdentifierToken: ( '`' IdentifierPart '`' | IdentifierPart)([.-] ( '`' IdentifierPart '`' | IdentifierPart) )*;
fragment IdentifierPart: [\p{XID_Start}] [\p{XID_Continue}]*;
// See discussion in AVRO-1022, AVRO-2659, AVRO-3115
// fragment IdentifierPart: [A-Za-z] [A-Za-z0-9_]*
