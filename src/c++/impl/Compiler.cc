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

//#define DEBUG_VERBOSE

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

        NodePtr &owner = stack_.back();

        owner->addLeaf(node);
        if(owner->type() == AVRO_RECORD) {
            owner->addName(fieldName_);
        }   
    }   
}

void 
CompilerContext::addCompound(const NodePtr &node)
{
    add(node);
    stack_.push_back(node);
}

void
CompilerContext::endCompound(Type type)
{
#ifdef DEBUG_VERBOSE
    std::cout << "Got end of " << type << '\n';
#endif
    assert(!stack_.empty());
    stack_.pop_back();
    inEnum_ = false;
}

void 
CompilerContext::addRecord()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding record " << text_ << '\n';
#endif
    NodePtr node(new NodeRecord());
    node->setName(text_);
    addCompound(node);
}

void 
CompilerContext::addEnum()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding enum " << text_ << '\n';
#endif
    NodePtr node(new NodeEnum());
    node->setName(text_);
    addCompound(node);
    inEnum_ = true;
}

void 
CompilerContext::addUnion()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding union\n";
#endif
    NodePtr node(new NodeUnion());
    addCompound(node);
}

void 
CompilerContext::addMap()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding map\n";
#endif
    NodePtr node(new NodeMap());
    addCompound(node);
}

void 
CompilerContext::addArray()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding array\n";
#endif
    NodePtr node(new NodeArray());
    addCompound(node);
}

void 
CompilerContext::addFixed()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding fixed " << text_ << '\n';
#endif
    NodePtr node(new NodeFixed());
    node->setName(text_);
    node->setFixedSize(size_);
    add(node);
} 

void 
CompilerContext::addPrimitive(Type type)
{    
#ifdef DEBUG_VERBOSE
    std::cout << "Adding " << type << '\n';
#endif
    NodePtr node(new NodePrimitive(type));
    add(node);
}

void 
CompilerContext::addSize()
{
    size_ = atol(text_.c_str()); 
#ifdef DEBUG_VERBOSE
    std::cout << "Got size " << size_ << '\n';
#endif
}

void 
CompilerContext::addSymbol()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Adding symbol " << text_ << '\n';
#endif
    NodePtr node(new NodeSymbolic());
    node->setName(text_);
    add(node);
}

void 
CompilerContext::addName()
{
    if(inEnum_) {
#ifdef DEBUG_VERBOSE
        std::cout << "Got enum symbol " << text_ << '\n';
#endif
        stack_.back()->addName(text_);
    }
}

void 
CompilerContext::addFieldName()
{
#ifdef DEBUG_VERBOSE
    std::cout << "Got field name " << text_ << '\n';
#endif
    fieldName_ = text_;
}

} // namespace avro
