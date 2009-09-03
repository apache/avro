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

#include "Compiler.hh"
#include "InputStreamer.hh"
#include "Types.hh"
#include "Schema.hh"
#include "ValidSchema.hh"

extern void yyparse(void *ctx);

namespace avro {

 #define DEBUG_VERBOSE

int
compileJsonSchema(std::istream &is, ValidSchema &schema)
{
     CompilerContext myctx(is);
     yyparse(&myctx);

     Schema s(myctx.getRoot());

     schema.setSchema(s);

     return 1;
}

void 
CompilerContext::add(const NodePtr &node)
{
    if(stack_.empty() ) {
        root_ = node;
    }
    else {
        stack_.back().addNode(node);
    }   
}

void
CompilerContext::startType()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Start type definition\n";
#endif
    stack_.push_back(new CompilerNode());
}

void
CompilerContext::stopType()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Stop type " << stack_.back().type() << '\n';
#endif

    assert(!stack_.empty());
    NodePtr nodePtr(nodeFromCompilerNode(stack_.back()));
    stack_.pop_back();
    add(nodePtr);
}

void 
CompilerContext::addType(Type type)
{    
#ifdef DEBUG_VERBOSE
    std::cerr << "Setting type to " << type << '\n';
#endif
    stack_.back().setType(type);
}

void 
CompilerContext::setSizeAttribute()
{
    int size = atol(text_.c_str()); 
#ifdef DEBUG_VERBOSE
    std::cerr << "Setting size to " << size << '\n';
#endif
    stack_.back().sizeAttribute_.add(size);
}

void 
CompilerContext::addNamedType()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Adding named type " << text_ << '\n';
#endif
    stack_.back().setType(AVRO_SYMBOLIC);
    stack_.back().nameAttribute_.add(text_);
}

void 
CompilerContext::setNameAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Setting name to " << text_ << '\n';
#endif
    stack_.back().nameAttribute_.add(text_);
}

void 
CompilerContext::setSymbolsAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Adding enum symbol " << text_ << '\n';
#endif
    stack_.back().symbolsAttribute_.add(text_);
}

void 
CompilerContext::setValuesAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Ready for map type\n";
#endif
    stack_.back().setAttributeType(CompilerNode::VALUES);
}

void 
CompilerContext::setTypesAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Ready for union types\n";
#endif
    stack_.back().setAttributeType(CompilerNode::TYPES);
}

void 
CompilerContext::setItemsAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Ready for array type\n";
#endif
    stack_.back().setAttributeType(CompilerNode::ITEMS);
}

void 
CompilerContext::setFieldsAttribute()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Ready for record fields\n";
#endif
    stack_.back().setAttributeType(CompilerNode::FIELDS);
}

void 
CompilerContext::textContainsFieldName()
{
#ifdef DEBUG_VERBOSE
    std::cerr << "Setting field name to " << text_ << '\n';
#endif
    stack_.back().fieldsNamesAttribute_.add(text_);
}

} // namespace avro
