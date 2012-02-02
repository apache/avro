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

#include "Node.hh"

namespace avro {

using std::string;

Node::~Node()
{ }

void 
Node::checkName(const std::string &name) const
{
    string::const_iterator it = name.begin();
    if (it != name.end() && (isalpha(*it) || *it == '_')) {
        while ((++it != name.end()) && (isalnum(*it) || *it == '_'));
        if (it == name.end()) {
            return;
        }
    }
    throw Exception("Names must match [A-Za-z_][A-Za-z0-9_]*");
}

} // namespace avro
