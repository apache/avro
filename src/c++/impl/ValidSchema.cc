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

#include <boost/format.hpp>

#include "ValidSchema.hh"
#include "SymbolMap.hh"
#include "Schema.hh"
#include "Node.hh"

namespace avro {

    ValidSchema::ValidSchema(const Schema &schema) :
    root_(schema.root())
{
    SymbolMap symbolMap;
    validate(root_, symbolMap);
}

ValidSchema::ValidSchema() :
   root_(NullSchema().root()) 
{ }

void
ValidSchema::setSchema(const Schema &schema)
{
    const NodePtr &node(schema.root());
    SymbolMap symbolMap;
    validate(schema.root(), symbolMap);
    root_ = node;
}

bool
ValidSchema::validate(const NodePtr &node, SymbolMap &symbolMap) 
{
    if(!node) {
        root_.reset(new NodePrimitive(AVRO_NULL));
    }

    if(!node->isValid()) {
        throw Exception( boost::format("Schema is invalid, due to bad node of type %1%") % node->type());
    }
    if(node->hasName()) {
        if(node->type() == AVRO_SYMBOLIC) {
            if(!symbolMap.hasSymbol(node->name())) {
                throw Exception( boost::format("Symbolic name \"%1%\" is unknown") % node->name());
            }

            boost::shared_ptr<NodeSymbolic> symNode = boost::static_pointer_cast<NodeSymbolic>(node);

            // if the symbolic link is already resolved, we return true,
            // otherwise returning false will force it to be resolved
            return symNode->isSet();
        }
        bool registered = symbolMap.registerSymbol(node);
        if(!registered) {
            return false;
        }
    }
    node->lock();
    size_t leaves = node->leaves();
    for(size_t i = 0; i < leaves; ++i) {
        const NodePtr &leaf(node->leafAt(i));

        if(! validate(leaf, symbolMap)) {

            // if validate returns false it means a node with this name already
            // existed in the map, instead of keeping this node twice in the
            // map (which could potentially create circular shared pointer
            // links that could not be easily freed), replace this node with a
            // symbolic link to the original one.
            
            NodePtr redirect = symbolMap.locateSymbol(leaf->name());
            node->setLeafToSymbolic(i, redirect);
        }
    }

    return true;
}

void 
ValidSchema::toJson(std::ostream &os) const
{ 
    root_->printJson(os, 0);
    os << '\n';
}

void 
ValidSchema::toFlatList(std::ostream &os) const
{ 
    root_->printBasicInfo(os);
}

} // namespace avro

