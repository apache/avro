/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/format.hpp>
#include <cctype>
#include <sstream>

#include "ValidSchema.hh"
#include "Schema.hh"
#include "Node.hh"

using std::string;
using std::ostringstream;
using std::make_pair;
using boost::format;
using std::shared_ptr;
using std::static_pointer_cast;

namespace avro {
typedef std::map<Name, NodePtr> SymbolMap;

static bool validate(const NodePtr &node, SymbolMap &symbolMap)
{
    if (! node->isValid()) {
        throw Exception(format("Schema is invalid, due to bad node of type %1%")
            % node->type());
    }

    if (node->hasName()) {
        const Name& nm = node->name();
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

string
ValidSchema::toJson(bool prettyPrint) const
{
    ostringstream oss;
    toJson(oss);
    if (!prettyPrint) {
        return compactSchema(oss.str());
    }
    return oss.str();
}

void
ValidSchema::toFlatList(std::ostream &os) const
{
    root_->printBasicInfo(os);
}

/*
 * compactSchema compacts and returns a formatted string representation
 * of a ValidSchema object by removing the whitespaces outside of the quoted
 * field names and values. It can handle the cases where the quoted value is
 * in UTF-8 format. Note that this method is not responsible for validating
 * the schema.
 */
string ValidSchema::compactSchema(const string& schema) {
    bool insideQuote = false;
    size_t newPos = 0;
    string data(schema.data());

    for (size_t currentPos = 0; currentPos < schema.size(); currentPos++) {
        if (!insideQuote && std::isspace(data[currentPos])) {
            // Skip the white spaces outside quotes.
            continue;
        }

        if (data[currentPos] == '\"') {
            // It is valid for a quote to be part of the value for some fields,
            // e.g., the "doc" field.  In that case, the quote is expected to be
            // escaped inside the schema.  Since the escape character '\\' could
            // be escaped itself, we need to check whether there are an even
            // number of consecutive slashes prior to the quote.
            int leadingSlashes = 0;
            for (int i = newPos - 1; i >= 0; i--) {
                if (data[i] == '\\') {
                    leadingSlashes++;
                } else {
                    break;
                }
            }
            if (leadingSlashes % 2 == 0) {
                // Found a real quote which identifies either the start or the
                // end of a field name or value.
                insideQuote = !insideQuote;
            }
        }
        data[newPos++] = data[currentPos];
    }

    if (insideQuote) {
        throw Exception("Schema is not well formed with mismatched quotes");
    }

    if (newPos < schema.size()) {
        data.resize(newPos);
    }
    return data;
}

} // namespace avro

