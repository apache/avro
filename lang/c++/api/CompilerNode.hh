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

#ifndef avro_CompilerNode_hh__
#define avro_CompilerNode_hh__

#include "NodeConcepts.hh"
#include "Node.hh"

namespace avro {


/// This is a generic "untyped" node that may store values for all possible
/// attributes of Avro complex types.  This allows a Node to be assembled by
/// the compiler, before it knows what attributes the Node actually contains.
/// All the Avro types defined (see NodeImpl) may be copy constructed from a
/// CompilerNode, at which time the attributes actually required by the Avro
/// type are copied from the CompilerNode, and all unused attributes are
/// dropped.

class CompilerNode
{

  public:

    enum AttributeType {
        NONE,
        FIELDS,
        VALUES,
        ITEMS,
        TYPES
    };

    CompilerNode() :
        type_(AVRO_NUM_TYPES),
        attributeType_(NONE)
    {}

    CompilerNode(const CompilerNode &rhs) :
        type_(rhs.type_),
        attributeType_(rhs.attributeType_)
    {}


    AttributeType attributeType() const {
        return attributeType_;
    }

    void setAttributeType(AttributeType attributeType) {
        attributeType_ = attributeType;
    }

    Type type() const {
        return type_;
    }

    void setType(Type type) {
        type_ = type;
    } 

    void addNode(const NodePtr &node) {
        switch(attributeType_) {
          case FIELDS:
            fieldsAttribute_.add(node);
            break;
          case VALUES:
            valuesAttribute_.add(node);
            break;
          case ITEMS:
            itemsAttribute_.add(node);
            break;
          case TYPES:
            typesAttribute_.add(node);
            break;

          default:
            throw Exception("Can't add node if the attribute type is not set");
        }
    }


    // attribute used by records, enums, symbols, and fixed:
    concepts::SingleAttribute<std::string> nameAttribute_;

    // attribute used by fixed:
    concepts::SingleAttribute<int> sizeAttribute_;

    // attributes used by records:
    concepts::MultiAttribute<NodePtr>     fieldsAttribute_;
    concepts::MultiAttribute<std::string> fieldsNamesAttribute_;

    // attribute used by enums:
    concepts::MultiAttribute<std::string> symbolsAttribute_;

    // attribute used by arrays:
    concepts::SingleAttribute<NodePtr> itemsAttribute_;

    // attribute used by maps:
    concepts::SingleAttribute<NodePtr> valuesAttribute_;

    // attribute used by unions:
    concepts::MultiAttribute<NodePtr> typesAttribute_;

    Type type_;
    AttributeType attributeType_;

};

NodePtr nodeFromCompilerNode(CompilerNode &compilerNode);

} // namespace avro

#endif
