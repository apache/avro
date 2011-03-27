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

#include "Generic.hh"
#include "NodeImpl.hh"
#include <sstream>

namespace avro {

using std::string;
using std::vector;
using std::ostringstream;

typedef vector<uint8_t> bytes;

void GenericContainer::assertType(const NodePtr& schema, Type type,
    const char* message)
{
    if (schema->type() != type) {
        throw Exception(message);
    }
}

GenericDatum::GenericDatum(const NodePtr& schema) : type_(schema->type())
{
    if (type_ == AVRO_SYMBOLIC) {
        type_ = static_cast<NodeSymbolic&>(*schema).type();
    }
    switch (type_) {
        case AVRO_NULL:
            break;
        case AVRO_BOOL:
            value_ = bool();
            break;
        case AVRO_INT:
            value_ = int32_t();
            break;
        case AVRO_LONG:
            value_ = int64_t();
            break;
        case AVRO_FLOAT:
            value_ = float();
            break;
        case AVRO_DOUBLE:
            value_ = double();
            break;
        case AVRO_STRING:
            value_ = string();
            break;
        case AVRO_BYTES:
            value_ = vector<uint8_t>();
            break;
        case AVRO_FIXED:
            value_ = GenericFixed(schema);
            break;
        case AVRO_RECORD:
            value_ = GenericRecord(schema);
            break;
        case AVRO_ENUM:
            value_ = GenericEnum(schema);
            break;
        case AVRO_ARRAY:
            value_ = GenericArray(schema);
            break;
        case AVRO_MAP:
            value_ = GenericMap(schema);
            break;
        case AVRO_UNION:
            throw Exception("Generic datum cannot be a union");
        default:
            throw Exception(boost::format("Unknown schema type %1%") %
                toString(type_));
    }
}

GenericRecord::GenericRecord(const NodePtr& schema) : GenericContainer(schema) {
    fields_.resize(schema->leaves());
}

GenericReader::GenericReader(const ValidSchema& s, const DecoderPtr& decoder) :
    schema_(s), isResolving_(dynamic_cast<ResolvingDecoder*>(&(*decoder)) != 0),
    decoder_(decoder)
{
}

GenericReader::GenericReader(const ValidSchema& writerSchema,
    const ValidSchema& readerSchema, const DecoderPtr& decoder) :
    schema_(readerSchema),
    isResolving_(true),
    decoder_(resolvingDecoder(writerSchema, readerSchema, decoder))
{
}

void GenericReader::read(GenericDatum& datum) const
{
    read(datum, schema_.root(), *decoder_, isResolving_);
}

static void ensureType(GenericDatum& datum, const NodePtr& n)
{
    if (datum.type() != n->type()) {
        switch (n->type()) {
        case AVRO_NULL:
            datum = GenericDatum();
            break;
        case AVRO_BOOL:
            datum = bool();
            break;
        case AVRO_INT:
            datum = int32_t();
            break;
        case AVRO_LONG:
            datum = int64_t();
            break;
        case AVRO_FLOAT:
            datum = float();
            break;
        case AVRO_DOUBLE:
            datum = double();
            break;
        case AVRO_STRING:
            datum = string();
            break;
        case AVRO_BYTES:
            datum = bytes();
            break;
        case AVRO_FIXED:
        case AVRO_RECORD:
        case AVRO_ENUM:
        case AVRO_ARRAY:
        case AVRO_MAP:
            datum = n;
            break;
        case AVRO_UNION:
            break;
        default:
            throw Exception("Unknown schema type");
        }
    }
}

void GenericReader::read(GenericDatum& datum, const NodePtr& n, Decoder& d,
    bool isResolving)
{
    NodePtr nn = n;
    if (nn->type() == AVRO_UNION) {
        size_t r = d.decodeUnionIndex();
        nn = nn->leafAt(r);
    }
    if (nn->type() == AVRO_SYMBOLIC) {
        nn = static_cast<NodeSymbolic&>(*nn).getNode();
    }
    ensureType(datum, nn);
    switch (nn->type()) {
    case AVRO_NULL:
        d.decodeNull();
        break;
    case AVRO_BOOL:
        datum.value<bool>() = d.decodeBool();
        break;
    case AVRO_INT:
        datum.value<int32_t>() = d.decodeInt();
        break;
    case AVRO_LONG:
        datum.value<int64_t>() = d.decodeLong();
        break;
    case AVRO_FLOAT:
        datum.value<float>() = d.decodeFloat();
        break;
    case AVRO_DOUBLE:
        datum.value<double>() = d.decodeDouble();
        break;
    case AVRO_STRING:
        d.decodeString(datum.value<string>());
        break;
    case AVRO_BYTES:
        d.decodeBytes(datum.value<bytes>());
        break;
    case AVRO_FIXED:
        d.decodeFixed(nn->fixedSize(), datum.value<GenericFixed>().value());
        break;
    case AVRO_RECORD:
        {
            GenericRecord& r = datum.value<GenericRecord>();
            size_t c = nn->leaves();
            if (isResolving) {
                std::vector<size_t> fo =
                    static_cast<ResolvingDecoder&>(d).fieldOrder();
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(fo[i]), nn->leafAt(fo[i]), d, isResolving);
                }
            } else {
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(i), nn->leafAt(i), d, isResolving);
                }
            }
        }
        break;
    case AVRO_ENUM:
        datum.value<GenericEnum>().set(d.decodeEnum());
        break;
    case AVRO_ARRAY:
        {
            vector<GenericDatum>& r = datum.value<GenericArray>().value();
            r.resize(0);
            size_t start = 0;
            for (size_t m = d.arrayStart(); m != 0; m = d.arrayNext()) {
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    read(r[start], nn->leafAt(0), d, isResolving);
                }
            }
        }
        break;
    case AVRO_MAP:
        {
            GenericMap::Value& r = datum.value<GenericMap>().value();
            r.resize(0);
            size_t start = 0;
            for (size_t m = d.mapStart(); m != 0; m = d.mapNext()) {
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    d.decodeString(r[start].first);
                    read(r[start].second, nn->leafAt(1), d, isResolving);
                }
            }
        }
        break;
    default:
        throw Exception("Unknown schema type");
    }
}

void GenericReader::read(Decoder& d, GenericDatum& g, const ValidSchema& s)
{
    read(g, s.root(), d, dynamic_cast<ResolvingDecoder*>(&d) != 0);
}

static void typeMismatch(Type t, Type u)
{
    throw Exception(boost::format("Type mismatch %1% v %2%") %
        toString(t) % toString(u));
}

template <typename T>
bool hasSameName(const GenericDatum& datum, const NodePtr& n)
{
    const T& c = datum.value<T>();
    return c.schema()->name() == n->name();
}

template <typename T>
void assertSameType(const GenericDatum& datum, const NodePtr& n)
{
    const T& c = datum.value<T>();
    if (c.schema() != n) {
        typeMismatch(c.schema()->type(), n->type());
    }
}

static void assertType(const GenericDatum& datum, const NodePtr& n)
{
    if (datum.type() == n->type()) {
        switch (n->type()) {
        case AVRO_FIXED:
            assertSameType<GenericFixed>(datum, n);
            return;
        case AVRO_RECORD:
            assertSameType<GenericRecord>(datum, n);
            return;
        case AVRO_ENUM:
            assertSameType<GenericEnum>(datum, n);
            return;
        case AVRO_NULL:
        case AVRO_BOOL:
        case AVRO_INT:
        case AVRO_LONG:
        case AVRO_FLOAT:
        case AVRO_DOUBLE:
        case AVRO_STRING:
        case AVRO_BYTES:
        case AVRO_ARRAY:
        case AVRO_MAP:
            return;
        }
    }
    typeMismatch(datum.type(), n->type());
}

GenericWriter::GenericWriter(const ValidSchema& s, const EncoderPtr& encoder) :
    schema_(s), encoder_(encoder)
{
}

void GenericWriter::write(const GenericDatum& datum) const
{
    write(datum, schema_.root(), *encoder_);
}

static size_t selectBranch(const GenericDatum& datum, const NodePtr& n)
{
    size_t c = n->leaves();
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        if (datum.type() == nn->type()) {
            switch (datum.type()) {
            case AVRO_FIXED:
                if (hasSameName<GenericFixed>(datum, nn)) return i;
                break;
            case AVRO_RECORD:
                if (hasSameName<GenericRecord>(datum, nn)) return i;
                break;
            case AVRO_ENUM:
                if (hasSameName<GenericEnum>(datum, nn)) return i;
                break;
            default:
                return i;
            }
        }
    }
    ostringstream oss;
    n->printJson(oss, 0);
    throw Exception(boost::format("No match for %1% in %2%") %
        toString(datum.type()) % oss.str());
}

void GenericWriter::write(const GenericDatum& datum,
    const NodePtr& n, Encoder& e)
{
    NodePtr nn = n;
    if (nn->type() == AVRO_UNION) {
        size_t br = selectBranch(datum, nn);
        e.encodeUnionIndex(br);
        nn = nn->leafAt(br);
    }
    if (nn->type() == AVRO_SYMBOLIC) {
        nn = static_cast<NodeSymbolic&>(*nn).getNode();
    }
    assertType(datum, nn);
    switch (nn->type()) {
    case AVRO_NULL:
        e.encodeNull();
        break;
    case AVRO_BOOL:
        e.encodeBool(datum.value<bool>());
        break;
    case AVRO_INT:
        e.encodeInt(datum.value<int32_t>());
        break;
    case AVRO_LONG:
        e.encodeLong(datum.value<int64_t>());
        break;
    case AVRO_FLOAT:
        e.encodeFloat(datum.value<float>());
        break;
    case AVRO_DOUBLE:
        e.encodeDouble(datum.value<double>());
        break;
    case AVRO_STRING:
        e.encodeString(datum.value<string>());
        break;
    case AVRO_BYTES:
        e.encodeBytes(datum.value<bytes>());
        break;
    case AVRO_FIXED:
        e.encodeFixed(datum.value<GenericFixed>().value());
        break;
    case AVRO_RECORD:
        {
            const GenericRecord& r = datum.value<GenericRecord>();
            size_t c = nn->leaves();
            for (size_t i = 0; i < c; ++i) {
                write(r.fieldAt(i), nn->leafAt(i), e);
            }
        }
        break;
    case AVRO_ENUM:
        e.encodeEnum(datum.value<GenericEnum>().value());
        break;
    case AVRO_ARRAY:
        {
            const GenericArray::Value& r = datum.value<GenericArray>().value();
            e.arrayStart();
            if (! r.empty()) {
                e.setItemCount(r.size());
                for (GenericArray::Value::const_iterator it = r.begin();
                    it != r.end(); ++it) {
                    e.startItem();
                    write(*it, nn->leafAt(0), e);
                }
            }
            e.arrayEnd();
        }
        break;
    case AVRO_MAP:
        {
            const GenericMap::Value& r = datum.value<GenericMap>().value();
            e.mapStart();
            if (! r.empty()) {
                e.setItemCount(r.size());
                for (GenericMap::Value::const_iterator it = r.begin();
                    it != r.end(); ++it) {
                    e.startItem();
                    e.encodeString(it->first);
                    write(it->second, nn->leafAt(1), e);
                }
            }
            e.mapEnd();
        }
        break;
    default:
        throw Exception("Unknown schema type");
    }
}

void GenericWriter::write(Encoder& e, const GenericDatum& g,
    const ValidSchema& s)
{
    write(g, s.root(), e);
}

}   // namespace avro
