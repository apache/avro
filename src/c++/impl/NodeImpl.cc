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


#include "NodeImpl.hh"

namespace avro {

/// Wrap an indentation in a struct for ostream operator<< 
struct indent { 
    indent(int depth) :
        d(depth)
    { }
    int d; 
};

/// ostream operator for indent
std::ostream& operator <<(std::ostream &os, indent x)
{
    static const std::string spaces("    ");
    while(x.d--) {
        os << spaces; 
    }
    return os;
}

void 
NodePrimitive::printJson(std::ostream &os, int depth) const
{
    os << '\"' << type() << '\"';
}

void 
NodeSymbolic::printJson(std::ostream &os, int depth) const
{
    os << '\"' << nameAttribute_.get() << '\"';
}

void 
NodeRecord::printJson(std::ostream &os, int depth) const
{
    os << "{\n";
    os << indent(++depth) << "\"type\": \"record\",\n";
    if(!nameAttribute_.get().empty()) {
        os << indent(depth) << "\"name\": \"" << nameAttribute_.get() << "\",\n";
    }
    os << indent(depth) << "\"fields\": [\n";

    int fields = leafAttributes_.size();
    ++depth;
    for(int i = 0; i < fields; ++i) {
        if(i > 0) {
            os << indent(depth) << "},\n";
        }
        os << indent(depth) << "{\n";
        os << indent(++depth) << "\"name\": \"" << leafNameAttributes_.get(i) << "\",\n";
        os << indent(depth) << "\"type\": ";
        leafAttributes_.get(i)->printJson(os, depth);
        os << '\n';
        --depth;
    }
    os << indent(depth) << "}\n";
    os << indent(--depth) << "]\n";
    os << indent(--depth) << '}';
}

void 
NodeEnum::printJson(std::ostream &os, int depth) const
{
    os << "{\n";
    os << indent(++depth) << "\"type\": \"enum\",\n";
    if(!nameAttribute_.get().empty()) {
        os << indent(depth) << "\"name\": \"" << nameAttribute_.get() << "\",\n";
    }
    os << indent(depth) << "\"symbols\": [\n";

    int names = leafNameAttributes_.size();
    ++depth;
    for(int i = 0; i < names; ++i) {
        if(i > 0) {
            os << ",\n";
        }
        os << indent(depth) << '\"' << leafNameAttributes_.get(i) << '\"';
    }
    os << '\n';
    os << indent(--depth) << "]\n";
    os << indent(--depth) << '}';
}

void 
NodeArray::printJson(std::ostream &os, int depth) const
{
    os << "{\n";
    os << indent(depth+1) << "\"type\": \"array\",\n";
    os << indent(depth+1) <<  "\"items\": ";
    leafAttributes_.get()->printJson(os, depth);
    os << '\n';
    os << indent(depth) << '}';
}

void 
NodeMap::printJson(std::ostream &os, int depth) const
{
    os << "{\n";
    os << indent(depth+1) <<"\"type\": \"map\",\n";
    os << indent(depth+1) << "\"values\": ";
    leafAttributes_.get(1)->printJson(os, depth);
    os << '\n';
    os << indent(depth) << '}';
}

void 
NodeUnion::printJson(std::ostream &os, int depth) const
{
    os << "[\n";
    int fields = leafAttributes_.size();
    ++depth;
    for(int i = 0; i < fields; ++i) {
        if(i > 0) {
            os << ",\n";
        }
        os << indent(depth);
        leafAttributes_.get(i)->printJson(os, depth);
    }
    os << '\n';
    os << indent(--depth) << ']';
}

void 
NodeFixed::printJson(std::ostream &os, int depth) const
{
    os << "{\n";
    os << indent(++depth) << "\"type\": \"fixed\",\n";
    os << indent(depth) << "\"size\": " << sizeAttribute_.get() << ",\n";
    os << indent(depth) << "\"name\": " << nameAttribute_.get() << "\"\n";
    os << indent(--depth) << '}';
}

} // namespace avro
