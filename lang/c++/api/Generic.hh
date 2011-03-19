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

#ifndef avro_Generic_hh__
#define avro_Generic_hh__

#include <vector>
#include <map>
#include <string>

#include <boost/utility.hpp>
#include <boost/any.hpp>

#include "Node.hh"
#include "Types.hh"
#include "Encoder.hh"
#include "Decoder.hh"
#include "ValidSchema.hh"

namespace avro {

class GenericDatum {
    Type type_;
    boost::any value_;

    GenericDatum(Type t) : type_(t) { }

    template <typename T>
    GenericDatum(Type t, const T& v) : type_(t), value_(v) { }

public:
    Type type() const {
        return type_;
    }

    template<typename T>
    const T& value() const {
        return *boost::any_cast<T>(&value_);
    }

    template<typename T>
    T& value() {
        return *boost::any_cast<T>(&value_);
    }

    /// Makes a new AVRO_NULL datum.
    GenericDatum() : type_(AVRO_NULL) { }

    /// Makes a new AVRO_BOOL datum whose value is of type bool.
    GenericDatum(bool v) : type_(AVRO_BOOL), value_(v) { }

    /// Makes a new AVRO_INT datum whose value is of type int32_t.
    GenericDatum(int32_t v) : type_(AVRO_INT), value_(v) { }

    /// Makes a new AVRO_LONG datum whose value is of type int64_t.
    GenericDatum(int64_t v) : type_(AVRO_LONG), value_(v) { }

    /// Makes a new AVRO_FLOAT datum whose value is of type float.
    GenericDatum(float v) : type_(AVRO_FLOAT), value_(v) { }

    /// Makes a new AVRO_DOUBLE datum whose value is of type double.
    GenericDatum(double v) : type_(AVRO_DOUBLE), value_(v) { }

    /// Makes a new AVRO_STRING datum whose value is of type std::string.
    GenericDatum(const std::string& v) : type_(AVRO_STRING), value_(v) { }

    /// Makes a new AVRO_BYTES datum whose value is of type
    /// std::vector<uint8_t>.
    GenericDatum(const std::vector<uint8_t>& v) :
        type_(AVRO_BYTES), value_(v) { }

    GenericDatum(const NodePtr& schema);
};

class GenericContainer {
    const NodePtr schema_;
protected:
    GenericContainer(const NodePtr& s) : schema_(s) { }

    static void assertSameType(const GenericDatum& v, const NodePtr& n);
    static void assertType(const NodePtr& schema, Type type,
        const char* message);
public:
    /// Returns the schema for this object
    const NodePtr& schema() const {
        return schema_;
    }
};

class GenericRecord : public GenericContainer {
    std::vector<GenericDatum> fields_;
public:
    GenericRecord(const NodePtr& schema);

    size_t fieldCount() const {
        return fields_.size();
    }

    const GenericDatum& fieldAt(size_t pos) const {
        return fields_[pos];
    }

    GenericDatum& fieldAt(size_t pos) {
        return fields_[pos];
    }

    void setFieldAt(size_t pos, const GenericDatum& v) {
        assertSameType(v, schema()->leafAt(pos));    
        fields_[pos] = v;
    }
};

class GenericArray : public GenericContainer {
public:
    typedef std::vector<GenericDatum> Value;

    GenericArray(const NodePtr& schema) : GenericContainer(schema) {
        if (schema->type() != AVRO_ARRAY) {
            throw Exception("Schema is not an array");
        }
    }

    const Value& value() const {
        return value_;
    }

    Value& value() {
        return value_;
    }
private:
    Value value_;
};

class GenericMap : public GenericContainer {
public:
    typedef std::vector<std::pair<std::string, GenericDatum> > Value;

    GenericMap(const NodePtr& schema) : GenericContainer(schema) {
        assertType(schema, AVRO_MAP, "Schema is not a map");
    }

    const Value& value() const {
        return value_;
    }

    Value& value() {
        return value_;
    }
private:
    Value value_;
};

class GenericEnum : public GenericContainer {
    size_t value_;
public:
    GenericEnum(const NodePtr& schema) : GenericContainer(schema), value_(0) {
    }

    const std::string& symbol(size_t n) {
        if (n < schema()->names()) {
            return schema()->nameAt(n);
        }
        throw Exception("Not as many symbols");
    }

    size_t index(const std::string& symbol) const {
        size_t result;
        if (schema()->nameIndex(symbol, result)) {
            return result;
        }
        throw Exception("No such symbol");
    }

    size_t set(const std::string& symbol) {
        return value_ = index(symbol);
    }

    void set(size_t n) {
        if (n < schema()->names()) {
            value_ = n;
            return;
        }
        throw Exception("Not as many symbols");
    }

    size_t value() const {
        return value_;
    }

    const std::string& symbol() const {
        return schema()->nameAt(value_);
    }
};

class GenericFixed : public GenericContainer {
    std::vector<uint8_t> value_;
public:
    GenericFixed(const NodePtr& schema) : GenericContainer(schema) {
        value_.resize(schema->fixedSize());
    }

    const std::vector<uint8_t>& value() const {
        return value_;
    }

    std::vector<uint8_t>& value() {
        return value_;
    }
};


class GenericReader : boost::noncopyable {
    const ValidSchema schema_;
    const bool isResolving_;
    const DecoderPtr decoder_;

    static void read(GenericDatum& datum, const NodePtr& n, Decoder& d,
        bool isResolving);
public:
    GenericReader(const ValidSchema& s, const DecoderPtr& decoder);
    GenericReader(const ValidSchema& writerSchema,
        const ValidSchema& readerSchema, const DecoderPtr& decoder);

    void read(GenericDatum& datum) const;
};


class GenericWriter : boost::noncopyable {
    const ValidSchema schema_;
    const EncoderPtr encoder_;

    static void write(const GenericDatum& datum, const NodePtr& n, Encoder& e);
public:
    GenericWriter(const ValidSchema& s, const EncoderPtr& encoder);

    void write(const GenericDatum& datum) const;
};
}   // namespace avro
#endif

