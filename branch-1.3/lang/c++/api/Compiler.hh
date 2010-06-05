/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_Compiler_hh__
#define avro_Compiler_hh__

#include "Boost.hh"

#include <FlexLexer.h>
#include "Types.hh"
#include "Node.hh"
#include "CompilerNode.hh"

namespace avro {

/// This class is used to implement an avro spec parser using a flex/bison
/// compiler.  In order for the lexer to be reentrant, this class provides a
/// lexer object for each parse.  The bison parser also uses this class to
/// build up an avro parse tree as the avro spec is parsed.
    
class CompilerContext {


  public:

    CompilerContext(std::istream &is) :
        lexer_(&is)
    {}

    /// Called by the lexer whenever it encounters text that is not a symbol it recognizes
    /// (names, fieldnames, values to be converted to integers, etc).
    void setText(const char *text) {
        text_ = text;
    }

    void addNamedType();

    void startType();
    void stopType();

    void addType(avro::Type type);

    void setSizeAttribute();
    void setNameAttribute();
    void setSymbolsAttribute();

    void setFieldsAttribute();
    void setItemsAttribute();
    void setValuesAttribute();
    void setTypesAttribute();

    void textContainsFieldName();

    const FlexLexer &lexer() const {
        return lexer_;
    }
    FlexLexer &lexer() {
        return lexer_;
    }

    const NodePtr &getRoot() const {
        return root_;
    }

  private:

    typedef boost::ptr_vector<CompilerNode> Stack;

    void add(const NodePtr &node);

    yyFlexLexer lexer_;
    std::string text_;
    
    NodePtr   root_;
    Stack     stack_;
};

class ValidSchema;

/// Given a stream comtaining a JSON schema, compiles the schema to a
/// ValidSchema object.  Throws if the schema cannot be compiled to a valid
/// schema

void compileJsonSchema(std::istream &is, ValidSchema &schema);

/// Non-throwing version of compileJsonSchema.  
///
/// \return True if no error, false if error (with the error string set)
///

bool compileJsonSchema(std::istream &is, ValidSchema &schema, std::string &error);

} // namespace avro

#endif
