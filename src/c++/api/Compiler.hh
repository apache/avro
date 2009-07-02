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

#include <vector>

#include <FlexLexer.h>
#include "Types.hh"
#include "Node.hh"
#include "SymbolMap.hh"

namespace avro {

/// This class is used to implement an avro spec parser using a flex/bison
/// compiler.  In order for the lexer to be reentrant, this class provides a
/// lexer object for each parse.  The bison parser also uses this class to
/// build up an avro parse tree as the avro spec is parsed.
    
class CompilerContext {

  public:

    CompilerContext(std::istream &is) :
        lexer_(&is),
        size_(0),
        inEnum_(false)
    {}

    /// Called by the lexer whenever it encounters text that is not a symbol it recognizes
    /// (names, fieldnames, values to be converted to integers, etc).
    void setText(const char *text) {
        text_ = text;
    }

    void addRecord();
    void addEnum();
    void addArray();
    void addMap();
    void addUnion();
    void addFixed();

    void endCompound(avro::Type type);

    void addPrimitive(avro::Type type);
    void addSymbol();
    void addSize();

    void addName();
    void addFieldName();

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

    void add(const NodePtr &node);
    void addCompound(const NodePtr &node);

    yyFlexLexer lexer_;
    std::string text_;
    std::string fieldName_;
    int64_t     size_;
    bool        inEnum_;
    SymbolMap   map_;
    
    NodePtr root_;
    std::vector<NodePtr> stack_;
};

class ValidSchema;

int compileJsonSchema(std::istream &is, ValidSchema &schema);

} // namespace avro

#endif
