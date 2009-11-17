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

#ifndef avro_SymbolMap_hh__
#define avro_SymbolMap_hh__

#include <map>
#include <boost/noncopyable.hpp>

#include "Node.hh"
#include "Schema.hh"

namespace avro {

/// Avro schemas can include types that were previously defined with names in
/// the same avro schema.  In order to identify new types, they are stored in a
/// map so that the actual type may be identified by name.  This class
/// implements the symbolic name to node mapping.
///

class SymbolMap : private boost::noncopyable
{

  public:

    SymbolMap()
    {}

    bool registerSymbol(const NodePtr &node) {

        if(node->type() == AVRO_SYMBOLIC) {
            throw Exception("Node must not be a symbolic name");
        }
        const std::string name = node->name();
        if(name.empty()) {
            throw Exception("Node must have a name to be registered");
        }
        bool added = false;
        MapImpl::iterator lb = map_.lower_bound(name);

        if(lb == map_.end() || map_.key_comp()(name, lb->first)) {
            map_.insert(lb, std::make_pair(name, node));
            added = true; 
        }
        return added;
    }

    bool hasSymbol(const std::string &name) const {
        return map_.find(name) != map_.end();
    }

    NodePtr locateSymbol(const std::string &name) const {
        MapImpl::const_iterator iter = map_.find(name);
        return (iter == map_.end()) ? NodePtr() : iter->second;
    }

  private:

    typedef std::map<std::string, NodePtr> MapImpl;

    MapImpl map_;
};


} // namespace avro

#endif
