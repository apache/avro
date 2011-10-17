/*
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

#ifndef avro_ValidSchema_hh__ 
#define avro_ValidSchema_hh__ 

#include "Node.hh"

namespace avro {

class Schema;
class SymbolMap;

/// A ValidSchema is basically a non-mutable Schema that has passed some
/// minumum of sanity checks.  Once valididated, any Schema that is part of
/// this ValidSchema is considered locked, and cannot be modified (an attempt
/// to modify a locked Schema will throw).  Also, as it is validated, any
/// recursive duplications of schemas are replaced with symbolic links to the
/// original.
///
/// Once a Schema is converted to a valid schema it can be used in validating
/// parsers/serializers, converted to a json schema, etc.
///

class ValidSchema 
{
  public:

    explicit ValidSchema(const Schema &schema);
    ValidSchema(const ValidSchema &schema);
    ValidSchema();

    void setSchema(const Schema &schema);

    const NodePtr &root() const {
        return root_;
    }

    void toJson(std::ostream &os) const;

    void toFlatList(std::ostream &os) const;

  protected:

    bool validate(const NodePtr &node, SymbolMap &symbolMap);

    NodePtr root_;

  private:

    // not implemented, only copy construct allowed
    ValidSchema &operator=(const Schema &rhs);

};

} // namespace avro

#endif
