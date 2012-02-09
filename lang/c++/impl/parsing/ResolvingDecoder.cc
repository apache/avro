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

#define __STDC_LIMIT_MACROS

#include <string>
#include <stack>
#include <map>
#include <algorithm>
#include <ctype.h>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/any.hpp>
#include <boost/utility.hpp>

#include "ValidatingCodec.hh"
#include "Symbol.hh"
#include "Types.hh"
#include "ValidSchema.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "NodeImpl.hh"

namespace avro {

using boost::make_shared;

namespace parsing {

using boost::shared_ptr;
using boost::static_pointer_cast;

using std::map;
using std::pair;
using std::vector;
using std::string;
using std::reverse;
using std::ostringstream;
using std::istringstream;
using std::stack;
using std::find_if;
using std::make_pair;

typedef pair<NodePtr, NodePtr> NodePair;

class ResolvingGrammarGenerator : public ValidatingGrammarGenerator {
    Production doGenerate(const NodePtr& writer, const NodePtr& reader,
        map<NodePair, shared_ptr<Production> > &m,
        const map<NodePtr, shared_ptr<Production> > &m2);
    Production resolveRecords(const NodePtr& writer, const NodePtr& reader,
        map<NodePair, shared_ptr<Production> > &m,
        const map<NodePtr, shared_ptr<Production> > &m2);
    Production resolveUnion(const NodePtr& writer, const NodePtr& reader,
        map<NodePair, shared_ptr<Production> > &m,
        const map<NodePtr, shared_ptr<Production> > &m2);

    static vector<pair<string, size_t> > fields(const NodePtr& n) {
        vector<pair<string, size_t> > result;
        size_t c = n->names();
        for (size_t i = 0; i < c; ++i) {
            result.push_back(make_pair(n->nameAt(i), i));
        }
        return result;
    }

    static int bestBranch(const NodePtr& writer, const NodePtr& reader);

    Production getWriterProduction(const NodePtr& n,
        const map<NodePtr, shared_ptr<Production> >& m2);

public:
    Symbol generate(
        const ValidSchema& writer, const ValidSchema& reader);
};

Symbol ResolvingGrammarGenerator::generate(
    const ValidSchema& writer, const ValidSchema& reader) {
    map<NodePtr, shared_ptr<Production> > m2;

    const NodePtr& rr = reader.root();
    const NodePtr& rw = writer.root();
    Production backup = ValidatingGrammarGenerator::doGenerate(rw, m2);
    fixup(backup, m2);

    map<NodePair, shared_ptr<Production> > m;
    Production main = doGenerate(rr, rw, m, m2);
    fixup(main, m);
    return Symbol::rootSymbol(main, backup);
}

int ResolvingGrammarGenerator::bestBranch(const NodePtr& writer,
    const NodePtr& reader)
{
    Type t = writer->type();

    const size_t c = reader->leaves();
    for (size_t j = 0; j < c; ++j) {
        const NodePtr& r = reader->leafAt(j);
        if (t == r->type()) {
            if (r->hasName()) {
                if (r->name() == writer->name()) {
                    return j;
                }
            } else {
                return j;
            }
        }
    }

    for (size_t j = 0; j < c; ++j) {
        const NodePtr& r = reader->leafAt(j);
        Type rt = r->type();
        switch (t) {
        case AVRO_INT:
            if (rt == AVRO_LONG || rt == AVRO_DOUBLE || rt == AVRO_FLOAT) {
                return j;
            }
            break;
        case AVRO_LONG:
        case AVRO_FLOAT:
            if (rt == AVRO_DOUBLE) {
                return j;
            }
            break;
        default:
            break;
        }
    }
    return -1;
}

template<typename T1, typename T2>
struct equalsFirst
{
    const T1& v_;
    equalsFirst(const T1& v) : v_(v) { }
    bool operator()(const pair<T1, T2>& p) {
        return p.first == v_;
    }
};

Production ResolvingGrammarGenerator::getWriterProduction(const NodePtr& n,
    const map<NodePtr, shared_ptr<Production> >& m2)
{
    const NodePtr& nn = (n->type() == AVRO_SYMBOLIC) ?
        static_cast<const NodeSymbolic& >(*n).getNode() : n;
    map<NodePtr, shared_ptr<Production> >::const_iterator it2 = m2.find(nn);
    return (it2 != m2.end()) ? *(it2->second) :
        ValidatingGrammarGenerator::generate(nn);
}

Production ResolvingGrammarGenerator::resolveRecords(
    const NodePtr& writer, const NodePtr& reader,
    map<NodePair, shared_ptr<Production> >& m,
    const map<NodePtr, shared_ptr<Production> >& m2)
{
    Production result;

    vector<pair<string, size_t> > wf = fields(writer);
    vector<pair<string, size_t> > rf = fields(reader);
    vector<size_t> fieldOrder;
    fieldOrder.reserve(reader->names());

    for (vector<pair<string, size_t> >::const_iterator it = wf.begin();
        it != wf.end(); ++it) {
        vector<pair<string, size_t> >::iterator it2 =
            find_if(rf.begin(), rf.end(),
                equalsFirst<string, size_t>(it->first));
        if (it2 != rf.end()) {
            Production p = doGenerate(writer->leafAt(it->second),
                reader->leafAt(it2->second), m, m2);
            copy(p.rbegin(), p.rend(), back_inserter(result));
            fieldOrder.push_back(it2->second);
            rf.erase(it2);
        } else {
            Production p = getWriterProduction(writer->leafAt(it->second), m2);
            result.push_back(Symbol::skipStart());
            if (p.size() == 1) {
                result.push_back(p[0]);
            } else {
                result.push_back(Symbol::indirect(boost::make_shared<Production>(p)));
            } 
        }
    }

    if (! rf.empty()) {
        throw Exception("Don't know how to handle excess fields for reader.");
    }
    reverse(result.begin(), result.end());
    result.push_back(Symbol::sizeListAction(fieldOrder));
    result.push_back(Symbol::recordAction());

    return result;

}

Production ResolvingGrammarGenerator::resolveUnion(
    const NodePtr& writer, const NodePtr& reader,
    map<NodePair, shared_ptr<Production> >& m,
    const map<NodePtr, shared_ptr<Production> >& m2)
{
    vector<Production> v;
    size_t c = writer->leaves();
    v.reserve(c);
    for (size_t i = 0; i < c; ++i) {
        Production p = doGenerate(writer->leafAt(i), reader, m, m2);
        v.push_back(p);
    }
    Symbol r[] = {
        Symbol::alternative(v),
        Symbol::writerUnionAction()
    };
    return Production(r, r + 2);
}

Production ResolvingGrammarGenerator::doGenerate(
    const NodePtr& writer, const NodePtr& reader,
    map<NodePair, shared_ptr<Production> > &m,
    const map<NodePtr, shared_ptr<Production> > &m2)
{
    Type writerType = writer->type();
    Type readerType = reader->type();

    if (writerType == readerType) {
        switch (writerType) {
        case AVRO_NULL:
            return Production(1, Symbol::nullSymbol());
        case AVRO_BOOL:
            return Production(1, Symbol::boolSymbol());
        case AVRO_INT:
            return Production(1, Symbol::intSymbol());
        case AVRO_LONG:
            return Production(1, Symbol::longSymbol());
        case AVRO_FLOAT:
            return Production(1, Symbol::floatSymbol());
        case AVRO_DOUBLE:
            return Production(1, Symbol::doubleSymbol());
        case AVRO_STRING:
            return Production(1, Symbol::stringSymbol());
        case AVRO_BYTES:
            return Production(1, Symbol::bytesSymbol());
        case AVRO_FIXED:
            if (writer->name() == reader->name() &&
                writer->fixedSize() == reader->fixedSize()) {
                Symbol r[] = {
                    Symbol::sizeCheckSymbol(reader->fixedSize()),
                    Symbol::fixedSymbol() };
                Production result(r, r + 2);
                m[make_pair(writer, reader)] = boost::make_shared<Production>(result);
                return result;
            }
            break;
        case AVRO_RECORD:
            if (writer->name() == reader->name()) {
                const pair<NodePtr, NodePtr> key(writer, reader);
                m.erase(key);
                Production result = resolveRecords(writer, reader, m, m2);

                const bool found = m.find(key) != m.end();

                shared_ptr<Production> p = boost::make_shared<Production>(result);
                m[key] = p;
                return found ? Production(1, Symbol::indirect(p)) : result;
            }
            break;

        case AVRO_ENUM:
            if (writer->name() == reader->name()) {
                Symbol r[] = {
                    Symbol::enumAdjustSymbol(writer, reader),
                    Symbol::enumSymbol(),
                };
                Production result(r, r + 2);
                m[make_pair(writer, reader)] = boost::make_shared<Production>(result);
                return result;
            }
            break;

        case AVRO_ARRAY:
            {
                Production p = getWriterProduction(writer->leafAt(0), m2);
                Symbol r[] = {
                    Symbol::arrayEndSymbol(),
                    Symbol::repeater(
                        doGenerate(writer->leafAt(0), reader->leafAt(0), m, m2),
                        p, true),
                    Symbol::arrayStartSymbol() };
                return Production(r, r + 3);
            }
        case AVRO_MAP:
            {
                Production v = doGenerate(writer->leafAt(1),
                    reader->leafAt(1), m, m2);
                v.push_back(Symbol::stringSymbol());

                Production v2 = getWriterProduction(writer->leafAt(1), m2);
                v2.push_back(Symbol::stringSymbol());

                Symbol r[] = {
                    Symbol::mapEndSymbol(),
                    Symbol::repeater(v, v2, false),
                    Symbol::mapStartSymbol() };
                return Production(r, r + 3);
            }
        case AVRO_UNION:
            return resolveUnion(writer, reader, m, m2);
        case AVRO_SYMBOLIC:
            {
                shared_ptr<NodeSymbolic> w =
                    static_pointer_cast<NodeSymbolic>(writer);
                shared_ptr<NodeSymbolic> r =
                    static_pointer_cast<NodeSymbolic>(reader);
                NodePair p(w->getNode(), r->getNode());
                map<NodePair, shared_ptr<Production> >::iterator it = m.find(p);
                if (it != m.end() && it->second) {
                    return *it->second;
                } else {
                    m[p] = shared_ptr<Production>();
                    return Production(1, Symbol::placeholder(p));
                }
            }
        default:
            throw Exception("Unknown node type");
        }
    } else if (writerType == AVRO_UNION) {
        return resolveUnion(writer, reader, m, m2);
    } else {
        switch (readerType) {
        case AVRO_LONG:
            if (writerType == AVRO_INT) {
                return Production(1,
                    Symbol::resolveSymbol(Symbol::sInt, Symbol::sLong));
            }
            break;
        case AVRO_FLOAT:
            if (writerType == AVRO_INT || writerType == AVRO_LONG) {
                return Production(1,
                    Symbol::resolveSymbol(writerType == AVRO_INT ?
                    Symbol::sInt : Symbol::sLong, Symbol::sFloat));
            }
            break;
        case AVRO_DOUBLE:
            if (writerType == AVRO_INT || writerType == AVRO_LONG
                || writerType == AVRO_FLOAT) {
                return Production(1,
                    Symbol::resolveSymbol(writerType == AVRO_INT ?
                    Symbol::sInt : writerType == AVRO_LONG ?
                    Symbol::sLong : Symbol::sFloat, Symbol::sDouble));
            }
            break;

        case AVRO_UNION:
            {
                int j = bestBranch(writer, reader);
                if (j >= 0) {
                    Production p = doGenerate(writer, reader->leafAt(j), m, m2);
                    Symbol r[] = {
                        Symbol::unionAdjustSymbol(j, p),
                        Symbol::unionSymbol()
                    };
                    return Production(r, r + 2);
                }
            }
            break;
        case AVRO_NULL:
        case AVRO_BOOL:
        case AVRO_INT:
        case AVRO_STRING:
        case AVRO_BYTES:
        case AVRO_ENUM:
        case AVRO_ARRAY:
        case AVRO_MAP:
        case AVRO_RECORD:
            break;
        default:
            throw Exception("Unknown node type");
        }
    }
    return Production(1, Symbol::error(writer, reader));
}

class ResolvingDecoderHandler {
    Decoder& base_;
public:
    ResolvingDecoderHandler(Decoder& base) : base_(base) { }
    size_t handle(const Symbol& s) {
        switch (s.kind()) {
        case Symbol::sWriterUnion:
            return base_.decodeUnionIndex();
        default:
            return 0;
        }
    }
};

template <typename Parser>
class ResolvingDecoderImpl : public ResolvingDecoder
{
    const DecoderPtr base_;
    ResolvingDecoderHandler handler_;
    Parser parser_;

    void init(InputStream& is);
    void decodeNull();
    bool decodeBool();
    int32_t decodeInt();
    int64_t decodeLong();
    float decodeFloat();
    double decodeDouble();
    void decodeString(string& value);
    void skipString();
    void decodeBytes(vector<uint8_t>& value);
    void skipBytes();
    void decodeFixed(size_t n, vector<uint8_t>& value);
    void skipFixed(size_t n);
    size_t decodeEnum();
    size_t arrayStart();
    size_t arrayNext();
    size_t skipArray();
    size_t mapStart();
    size_t mapNext();
    size_t skipMap();
    size_t decodeUnionIndex();
    const vector<size_t>& fieldOrder();
public:
    ResolvingDecoderImpl(const ValidSchema& writer, const ValidSchema& reader,
        const DecoderPtr& base) :
        base_(base),
        handler_(*base_),
        parser_(ResolvingGrammarGenerator().generate(reader, writer),
            &(*base_), handler_)
    {
    }
};

template <typename P>
void ResolvingDecoderImpl<P>::init(InputStream& is)
{
    base_->init(is);
    parser_.reset();
}

template <typename P>
void ResolvingDecoderImpl<P>::decodeNull()
{
    parser_.advance(Symbol::sNull);
    base_->decodeNull();
}

template <typename P>
bool ResolvingDecoderImpl<P>::decodeBool()
{
    parser_.advance(Symbol::sBool);
    return base_->decodeBool();
}

template <typename P>
int32_t ResolvingDecoderImpl<P>::decodeInt()
{
    parser_.advance(Symbol::sInt);
    return base_->decodeInt();
}

template <typename P>
int64_t ResolvingDecoderImpl<P>::decodeLong()
{
    Symbol::Kind k = parser_.advance(Symbol::sLong);
    return k == Symbol::sInt ? base_->decodeInt() : base_->decodeLong();
}

template <typename P>
float ResolvingDecoderImpl<P>::decodeFloat()
{
    Symbol::Kind k = parser_.advance(Symbol::sFloat);
    return k == Symbol::sInt ? base_->decodeInt() :
        k == Symbol::sLong ? base_->decodeLong() :
        base_->decodeFloat();
}

template <typename P>
double ResolvingDecoderImpl<P>::decodeDouble()
{
    Symbol::Kind k = parser_.advance(Symbol::sDouble);
    return k == Symbol::sInt ? base_->decodeInt() :
        k == Symbol::sLong ? base_->decodeLong() :
        k == Symbol::sFloat ? base_->decodeFloat() :
        base_->decodeDouble();
}

template <typename P>
void ResolvingDecoderImpl<P>::decodeString(string& value)
{
    parser_.advance(Symbol::sString);
    base_->decodeString(value);
}

template <typename P>
void ResolvingDecoderImpl<P>::skipString()
{
    parser_.advance(Symbol::sString);
    base_->skipString();
}

template <typename P>
void ResolvingDecoderImpl<P>::decodeBytes(vector<uint8_t>& value)
{
    parser_.advance(Symbol::sBytes);
    base_->decodeBytes(value);
}

template <typename P>
void ResolvingDecoderImpl<P>::skipBytes()
{
    parser_.advance(Symbol::sBytes);
    base_->skipBytes();
}

template <typename P>
void ResolvingDecoderImpl<P>::decodeFixed(size_t n, vector<uint8_t>& value)
{
    parser_.advance(Symbol::sFixed);
    parser_.assertSize(n);
    return base_->decodeFixed(n, value);
}

template <typename P>
void ResolvingDecoderImpl<P>::skipFixed(size_t n)
{
    parser_.advance(Symbol::sFixed);
    parser_.assertSize(n);
    base_->skipFixed(n);
}

template <typename P>
size_t ResolvingDecoderImpl<P>::decodeEnum()
{
    parser_.advance(Symbol::sEnum);
    size_t n = base_->decodeEnum();
    return parser_.enumAdjust(n);
}

template <typename P>
size_t ResolvingDecoderImpl<P>::arrayStart()
{
    parser_.advance(Symbol::sArrayStart);
    size_t result = base_->arrayStart();
    if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sArrayEnd);
    } else {
        parser_.setRepeatCount(result);
    }
    return result;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::arrayNext()
{
    size_t result = base_->arrayNext();
    if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sArrayEnd);
    } else {
        parser_.setRepeatCount(result);
    }
    return result;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::skipArray()
{
    parser_.advance(Symbol::sArrayStart);
    size_t n = base_->skipArray();
    if (n == 0) {
        parser_.pop();
    } else {
        parser_.setRepeatCount(n);
        parser_.skip(*base_);
    }
    parser_.advance(Symbol::sArrayEnd);
    return 0;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::mapStart()
{
    parser_.advance(Symbol::sMapStart);
    size_t result = base_->mapStart();
    if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sMapEnd);
    } else {
        parser_.setRepeatCount(result);
    }
    return result;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::mapNext()
{
    size_t result = base_->mapNext();
    if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sMapEnd);
    } else {
        parser_.setRepeatCount(result);
    }
    return result;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::skipMap()
{
    parser_.advance(Symbol::sMapStart);
    size_t n = base_->skipMap();
    if (n == 0) {
        parser_.pop();
    } else {
        parser_.setRepeatCount(n);
        parser_.skip(*base_);
    }
    parser_.advance(Symbol::sMapEnd);
    return 0;
}

template <typename P>
size_t ResolvingDecoderImpl<P>::decodeUnionIndex()
{
    parser_.advance(Symbol::sUnion);
    return parser_.unionAdjust();
}

template <typename P>
const vector<size_t>& ResolvingDecoderImpl<P>::fieldOrder()
{
    parser_.advance(Symbol::sRecord);
    return parser_.sizeList();
}

}   // namespace parsing

ResolvingDecoderPtr resolvingDecoder(const ValidSchema& writer,
    const ValidSchema& reader, const DecoderPtr& base) {
    return make_shared<parsing::ResolvingDecoderImpl
        <parsing::SimpleParser<parsing::ResolvingDecoderHandler> > >(
        writer, reader, base);
}

}   // namespace avro

