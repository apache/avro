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

#ifndef avro_Node_hh__
#define avro_Node_hh__

#include <cassert>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#include "Exception.hh"
#include "Types.hh"

namespace avro {

class Node;

typedef boost::shared_ptr<Node> NodePtr;


/// Node is the building block for parse trees.  Each node represents an avro
/// type.  Compound types have leaf nodes that represent the types they are
/// composed of.
///
/// The user does not use the Node object directly, they interface with Schema
/// objects.
///
/// The Node object uses reference-counted pointers.  This is so that schemas 
/// may be reused in other other schemas, without needing to worry about memory
/// deallocation for nodes that are added to multiple schema parse trees.
///
/// Node has minimal implementation, serving as an abstract base class for
/// different node types.
///

class Node : private boost::noncopyable
{
  public:

    Node(Type type) :
        type_(type),
        refCount_(0),
        locked_(false)
    {}

    virtual ~Node();

    Type type() const {
        return type_;
    }

    void lock() {
        locked_ = true;
    }

    bool locked() const {
        return locked_;
    }

    virtual bool hasName() const = 0;

    void setName(const std::string &name) {
        checkLock();
        checkName(name);
        doSetName(name);
    }
    virtual const std::string &name() const = 0;

    void addLeaf(const NodePtr &newLeaf) {
        checkLock();
        doAddLeaf(newLeaf);
    }
    virtual size_t leaves() const = 0;
    virtual const NodePtr& leafAt(int index) const = 0;

    void addName(const std::string &name) {
        checkLock();
        checkName(name);
        doAddName(name);
    }
    virtual size_t names() const = 0;
    virtual const std::string &nameAt(int index) const = 0;

    void setFixedSize(int size) {
        checkLock();
        doSetFixedSize(size);
    }
    virtual int fixedSize() const = 0;

    virtual bool isValid() const = 0;

    virtual void printJson(std::ostream &os, int depth) const = 0;

    virtual void printBasicInfo(std::ostream &os) const = 0;

  protected:

    friend class ValidSchema;

    virtual void setLeafToSymbolic(int index) = 0;

    void checkLock() const {
        if(locked()) {
            throw Exception("Cannot modify locked schema");
        }
    }

    void checkName(const std::string &name) const;

    virtual void doSetName(const std::string &name) = 0;
    virtual void doAddLeaf(const NodePtr &newLeaf) = 0;
    virtual void doAddName(const std::string &name) = 0;
    virtual void doSetFixedSize(int size) = 0;

  private:

    const Type type_;
    int refCount_;
    bool locked_;
};

} // namespace avro

#endif
