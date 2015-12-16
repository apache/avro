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

Name::Name(const std::string& name)
{
    fullname(name);
}

const string Name::fullname() const
{
    return (ns_.empty()) ? simpleName_ : ns_ + "." + simpleName_;
}

void Name::fullname(const string& name)
{
    string::size_type n = name.find_last_of('.');
    if (n == string::npos) {
        simpleName_ = name;
        ns_.clear();
    } else {
        ns_ = name.substr(0, n);
        simpleName_ = name.substr(n + 1);
    }
    check();
}

bool Name::operator < (const Name& n) const
{
    return (ns_ < n.ns_) ? true :
        (n.ns_ < ns_) ? false :
        (simpleName_ < n.simpleName_);
}

static bool invalidChar1(char c)
{
    return !isalnum(c) && c != '_' && c != '.';
}

static bool invalidChar2(char c)
{
    return !isalnum(c) && c != '_';
}

void Name::check() const
{
    if (! ns_.empty() && (ns_[0] == '.' || ns_[ns_.size() - 1] == '.' || std::find_if(ns_.begin(), ns_.end(), invalidChar1) != ns_.end())) {
        throw Exception("Invalid namespace: " + ns_);
    }
    if (simpleName_.empty() || std::find_if(simpleName_.begin(), simpleName_.end(), invalidChar2) != simpleName_.end()) {
        throw Exception("Invalid name: " + simpleName_);
    }
}

bool Name::operator == (const Name& n) const
{
    return ns_ == n.ns_ && simpleName_ == n.simpleName_;
}

} // namespace avro
