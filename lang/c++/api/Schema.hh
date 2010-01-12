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

#ifndef avro_Schema_hh__ 
#define avro_Schema_hh__ 

#include "NodeImpl.hh"

/// \file
///
/// Schemas for representing all the avro types.  The compound schema objects
/// allow composition from other schemas.
///

namespace avro {

class ValidSchema;


/// The root Schema object is a base class.  Nobody constructs this class directly.

class Schema 
{
  public:

    virtual ~Schema();

    Type type() const {
        return node_->type();
    }

    const NodePtr &root() const {
        return node_;
    }

    NodePtr &root() {
        return node_;
    }

  protected:

    friend void compileJsonSchema(std::istream &is, ValidSchema &schema);

    Schema();
    explicit Schema(const NodePtr &node);
    explicit Schema(Node *node);

    NodePtr node_;
};

class NullSchema : public Schema
{
  public:
    NullSchema(): Schema(new NodePrimitive(AVRO_NULL)) {}
};

class BoolSchema : public Schema
{
  public:
    BoolSchema(): Schema(new NodePrimitive(AVRO_BOOL)) {}
};

class IntSchema : public Schema
{
  public:
    IntSchema(): Schema(new NodePrimitive(AVRO_INT)) {}
};

class LongSchema : public Schema
{
  public:
    LongSchema(): Schema(new NodePrimitive(AVRO_LONG)) {}
};

class FloatSchema : public Schema
{
  public:
    FloatSchema(): Schema(new NodePrimitive(AVRO_FLOAT)) {}
};

class DoubleSchema : public Schema
{
  public:
    DoubleSchema(): Schema(new NodePrimitive(AVRO_DOUBLE)) {}
};

class StringSchema : public Schema
{
  public:
    StringSchema(): Schema(new NodePrimitive(AVRO_STRING)) {}
};

class BytesSchema : public Schema
{
  public:
    BytesSchema(): Schema(new NodePrimitive(AVRO_BYTES)) {}
};

class RecordSchema : public Schema
{
  public:

    RecordSchema(const std::string &name);
    void addField(const std::string &name, const Schema &fieldSchema);
};

class EnumSchema : public Schema
{
  public:

    EnumSchema(const std::string &name);
    void addSymbol(const std::string &symbol);
};

class ArraySchema : public Schema
{
  public:

    ArraySchema(const Schema &itemsSchema);
};

class MapSchema : public Schema
{
  public:

    MapSchema(const Schema &valuesSchema);
};


class UnionSchema : public Schema
{
  public:

    UnionSchema();
    void addType(const Schema &typeSchema);
};

class FixedSchema : public Schema
{
  public:

    FixedSchema(int size, const std::string &name);
};

} // namespace avro

#endif
