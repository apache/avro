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
#include "Schema.hh"
#include "Node.hh"

using std::string;
using std::make_pair;
using boost::format;
using boost::shared_ptr;
using boost::static_pointer_cast;

namespace avro {

typedef std::map<std::string, NodePtr> SymbolMap;

static bool validate(const NodePtr &node, SymbolMap &symbolMap) 
{
    if (! node->isValid()) {
        throw Exception(format("Schema is invalid, due to bad node of type %1%")
            % node->type());
    }

    if (node->hasName()) {
        const string& nm = node->name();
        SymbolMap::iterator it = symbolMap.find(nm);
        bool found = it != symbolMap.end() && nm == it->first;

        if (node->type() == AVRO_SYMBOLIC) {
            if (! found) {
                throw Exception(format("Symbolic name \"%1%\" is unknown") %
                    node->name());
            }

            shared_ptr<NodeSymbolic> symNode =
                static_pointer_cast<NodeSymbolic>(node);

            // if the symbolic link is already resolved, we return true,
            // otherwise returning false will force it to be resolved
            return symNode->isSet();
        }

        if (found) {
            return false;
        }
        symbolMap.insert(it, make_pair(nm, node));
    }

    node->lock();
    size_t leaves = node->leaves();
    for (size_t i = 0; i < leaves; ++i) {
        const NodePtr &leaf(node->leafAt(i));

        if (! validate(leaf, symbolMap)) {

            // if validate returns false it means a node with this name already
            // existed in the map, instead of keeping this node twice in the
            // map (which could potentially create circular shared pointer
            // links that could not be easily freed), replace this node with a
            // symbolic link to the original one.
            
            node->setLeafToSymbolic(i, symbolMap.find(leaf->name())->second);
        }
    }

    return true;
}

static void validate(const NodePtr& p)
{
    SymbolMap m;
    validate(p, m);
}

ValidSchema::ValidSchema(const NodePtr &root) : root_(root)
{
    validate(root_);
}

ValidSchema::ValidSchema(const Schema &schema) : root_(schema.root())
{
    validate(root_);
}

ValidSchema::ValidSchema() : root_(NullSchema().root()) 
{
    validate(root_);
}

void
ValidSchema::setSchema(const Schema &schema)
{
    root_ = schema.root();
    validate(root_);
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

