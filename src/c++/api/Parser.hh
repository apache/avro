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

#ifndef avro_Parser_hh__
#define avro_Parser_hh__

#include "Reader.hh"
#include "ValidatingReader.hh"

namespace avro {

///
/// Class that wraps a reader or ValidatingReade with an interface that uses
/// explicit get* names instead of getValue
///

template<class Reader>
class Parser : private boost::noncopyable
{

  public:

    // Constructor only works with Writer
    explicit Parser(InputStreamer &in) :
        reader_(in)
    {}

    /// Constructor only works with ValidatingWriter
    Parser(const ValidSchema &schema, InputStreamer &in) :
        reader_(schema, in)
    {}

    void getNull() {
        Null null;
        reader_.getValue(null);
    }

    bool getBool() {
        bool val;
        reader_.getValue(val);
        return val;
    }

    int32_t getInt() {
        int32_t val;
        reader_.getValue(val);
        return val;
    }

    int64_t getLong() {
        int64_t val;
        reader_.getValue(val);
        return val;
    }

    float getFloat() {
        float val;
        reader_.getValue(val);
        return val;
    }

    double getDouble() {
        double val;
        reader_.getValue(val);
        return val;
    }

    void getString(std::string &val) {
        reader_.getValue(val);
    }

    void getBytes(std::vector<uint8_t> &val) {
        reader_.getBytes(val);
    }

    void getFixed(std::vector<uint8_t> &val, size_t size) {
        reader_.getFixed(val, size);
    }

    void getFixed(uint8_t *val, size_t size) {
        reader_.getFixed(val, size);
    }

    void getRecord() { 
        reader_.getRecord();
    }

    int64_t getArrayBlockSize() {
        return reader_.getArrayBlockSize();
    }

    int64_t getUnion() { 
        return reader_.getUnion();
    }

    int64_t getEnum() {
        return reader_.getEnum();
    }

    int64_t getMapBlockSize() {
        return reader_.getMapBlockSize();
    }

  private:

    friend Type nextType(Parser<ValidatingReader> &p);
    friend bool getCurrentRecordName(Parser<ValidatingReader> &p, std::string &name);
    friend bool getNextFieldName(Parser<ValidatingReader> &p, std::string &name);

    Reader reader_;

};

inline Type nextType(Parser<ValidatingReader> &p) {
    return p.reader_.nextType();
}

inline bool getCurrentRecordName(Parser<ValidatingReader> &p, std::string &name) {
    return p.reader_.getCurrentRecordName(name);
}

inline bool getNextFieldName(Parser<ValidatingReader> &p, std::string &name) {
    return p.reader_.getNextFieldName(name);
}

} // namespace avro

#endif
