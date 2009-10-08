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

#include <boost/static_assert.hpp>

#include "Validator.hh"
#include "ValidSchema.hh"

namespace avro {

Validator::Validator(const ValidSchema &schema) :
    schema_(schema),
    parseTree_(schema.root()),
    nextType_(AVRO_NULL),
    expectedTypesFlag_(0),
    compoundStarted_(false),
    waitingForCount_(false),
    count_(0)
{
    setupOperation(parseTree_);
}

void 
Validator::setWaitingForCount()
{
    waitingForCount_ = true;
    count_ = 0;
    expectedTypesFlag_ = typeToFlag(AVRO_INT) | typeToFlag(AVRO_LONG);
    nextType_ = AVRO_LONG;
}

void
Validator::recordAdvance()
{
    // record doesn't use this flag because it doesn't need to set
    // up anything at the start, but just clear it
    compoundStarted_ = false;

    // determine the next record entry to process
    size_t index = (compoundStack_.back().pos)++;

    const NodePtr &node = compoundStack_.back().node;
    if(index < node->leaves() ) {
        setupOperation(node->leafAt(index));
    }
    else {
        // done with this record, remove it from the processing stack
        compoundStack_.pop_back();
    }
}

void
Validator::enumAdvance()
{
    if(compoundStarted_) {
        setWaitingForCount();
        compoundStarted_ = false;
    }
    else {
        waitingForCount_ = false;
        compoundStack_.pop_back();
    }
}

void
Validator::countingAdvance()
{
    const NodePtr &node = compoundStack_.back().node;

    if(compoundStarted_) {
        setWaitingForCount();
        compoundStarted_ = false;
    }
    else if(waitingForCount_) {
        waitingForCount_ = false;
        if(count_ == 0) {
            compoundStack_.pop_back();
        }
        else {
            counters_.push_back(count_);
            setupOperation(node->leafAt(0));
        }
    }
    else {

        size_t index = ++(compoundStack_.back().pos);

        if(index < node->leaves() ) {
            setupOperation(node->leafAt(index));
        }
        else {
            compoundStack_.back().pos = 0;
            int count = --counters_.back();
            if(count == 0) {
                counters_.pop_back();
                compoundStarted_ = true;
                nextType_ = node->type();
                expectedTypesFlag_ = typeToFlag(nextType_);
            }
            else {
                setupOperation(node->leafAt(0));
            }
        }
    }
}

void
Validator::unionAdvance()
{
    if(compoundStarted_) {
        setWaitingForCount();
        compoundStarted_ = false;
    }
    else {
        waitingForCount_ = false;
        NodePtr node = compoundStack_.back().node;

        if(count_ < static_cast<int64_t>(node->leaves())) {
            compoundStack_.pop_back();
            setupOperation(node->leafAt(count_));
        }
        else {
            throw Exception("Union out of range");
        }
    }
}

void
Validator::fixedAdvance()
{
    compoundStarted_ = false;
    compoundStack_.pop_back();
}

int 
Validator::nextSizeExpected() const
{
    return compoundStack_.back().node->fixedSize();
}

void
Validator::advance()
{
    typedef void (Validator::*AdvanceFunc)();

    // only the compound types need advance functions here
    static const AdvanceFunc funcs[] = {
        0, // string
        0, // bytes
        0, // int
        0, // long
        0, // float
        0, // double
        0, // bool
        0, // null
        &Validator::recordAdvance,
        &Validator::enumAdvance,
        &Validator::countingAdvance,
        &Validator::countingAdvance,
        &Validator::unionAdvance,
        &Validator::fixedAdvance,
        0 // symbolic
    };
    BOOST_STATIC_ASSERT( (sizeof(funcs)/sizeof(AdvanceFunc)) == (AVRO_NUM_TYPES) );

    expectedTypesFlag_ = 0;
    // loop until we encounter a next expected type, or we've exited all compound types 
    while(!expectedTypesFlag_ && !compoundStack_.empty() ) {
    
        Type type = compoundStack_.back().node->type();

        AdvanceFunc func = funcs[type];

        // only compound functions are put on the status stack so it is ok to
        // assume that func is not null
        assert(func);

        ((this)->*(func))();
    }
}

void
Validator::advanceWithCount(int64_t count) 
{
    if(!waitingForCount_) {
        throw Exception("Not expecting count");
    }
    else if(count_ < 0) {
        throw Exception("Count cannot be negative");
    }
    count_ = count;

    advance();
}

void
Validator::setupFlag(Type type)
{
    // use flags instead of strictly types, so that we can be more lax about the type
    // (for example, a long should be able to accept an int type, but not vice versa)
    static const flag_t flags[] = {
        typeToFlag(AVRO_STRING) | typeToFlag(AVRO_BYTES),
        typeToFlag(AVRO_STRING) | typeToFlag(AVRO_BYTES),
        typeToFlag(AVRO_INT),
        typeToFlag(AVRO_INT) | typeToFlag(AVRO_LONG),
        typeToFlag(AVRO_FLOAT),
        typeToFlag(AVRO_DOUBLE),
        typeToFlag(AVRO_BOOL),
        typeToFlag(AVRO_NULL),
        typeToFlag(AVRO_RECORD),
        typeToFlag(AVRO_ENUM),
        typeToFlag(AVRO_ARRAY),
        typeToFlag(AVRO_MAP),
        typeToFlag(AVRO_UNION),
        typeToFlag(AVRO_FIXED),
        0
    };
    BOOST_STATIC_ASSERT( (sizeof(flags)/sizeof(flag_t)) == (AVRO_NUM_TYPES) );

    expectedTypesFlag_ = flags[type];
}

void
Validator::setupOperation(const NodePtr &node)
{
    nextType_ = node->type();

    if(nextType_ == AVRO_SYMBOLIC) {
        NodePtr symNode ( schema_.followSymbol(node->name()) );
        assert(symNode);
        return setupOperation(symNode);
    }

    assert(nextType_ < AVRO_NUM_TYPES);

    setupFlag(nextType_);

    if(!isPrimitive(nextType_)) {
        compoundStack_.push_back(CompoundType(node));
        compoundStarted_ = true;
    }
}

bool 
Validator::getCurrentRecordName(std::string &name) const
{
    bool found = false;
    name.clear();

    int idx = -1;
    // if the top of the stack is a record I want this record name
    if(!compoundStack_.empty() && (isPrimitive(nextType_) || nextType_ == AVRO_RECORD)) {
        idx = compoundStack_.size() -1;
    }
    else {
        idx = compoundStack_.size() -2;
    }
    
    if(idx >= 0 && compoundStack_[idx].node->type() == AVRO_RECORD) {
        name = compoundStack_[idx].node->name();
        found = true;
    }
    return found;
}

bool 
Validator::getNextFieldName(std::string &name) const
{
    bool found = false;
    name.clear();
    int idx = isCompound(nextType_) ? compoundStack_.size()-2 : compoundStack_.size()-1;
    if(idx >= 0 && compoundStack_[idx].node->type() == AVRO_RECORD) {
        size_t pos = compoundStack_[idx].pos-1;
        const NodePtr &node = compoundStack_[idx].node;
        if(pos>= 0 && pos < node->leaves()) {
            name = node->nameAt(pos);
            found = true;
        }
    }
    return found;
}

} // namespace avro
