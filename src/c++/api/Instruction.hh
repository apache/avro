
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

#ifndef avro_Instruction_hh__
#define avro_Instruction_hh__

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <stdint.h>
#include "Boost.hh"

/// \file Instruction.hh
///

namespace avro {
    
class ValidatingReader;
class ValidSchema;

class Offset : public boost::noncopyable {

  public:

    Offset(size_t offset) :
        offset_(offset)
    {}

    virtual ~Offset() {}

    size_t offset() const {
        return offset_;
    }

  private:

    const size_t offset_;
};

class CompoundOffset : public Offset {

  public:

    CompoundOffset(size_t offset) :
        Offset(offset)
    {}

    void add(Offset * setter) {
        setters_.push_back(setter);
    }

    const Offset &at (size_t idx) const {
        return setters_.at(idx);
    }

  private:

    typedef boost::ptr_vector<Offset> Offsets;
    Offsets setters_;
};

typedef boost::shared_ptr<Offset> OffsetPtr;

class Instruction
{

  public:

    virtual void parse(ValidatingReader &reader, uint8_t *address) const = 0;
    virtual ~Instruction() {}

};

typedef boost::shared_ptr<Instruction> DynamicParser;
DynamicParser buildDynamicParser(const ValidSchema &writer, const ValidSchema &reader, const OffsetPtr &offset);

} // namespace avro

#endif
