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
using std::vector;
using std::string;
using std::reverse;
using std::ostringstream;
using std::istringstream;
using std::stack;

class JsonGrammarGenerator : public ValidatingGrammarGenerator {
    Production doGenerate(const NodePtr& n,
        std::map<NodePtr, boost::shared_ptr<Production> > &m);
};

static std::string nameOf(const NodePtr& n)
{
    if (n->hasName()) {
        return n->name();
    }
    std::ostringstream oss;
    oss << n->type();
    return oss.str();
}

Production JsonGrammarGenerator::doGenerate(const NodePtr& n,
    std::map<NodePtr, boost::shared_ptr<Production> > &m) {
    switch (n->type()) {
    case AVRO_NULL:
    case AVRO_BOOL:
    case AVRO_INT:
    case AVRO_LONG:
    case AVRO_FLOAT:
    case AVRO_DOUBLE:
    case AVRO_STRING:
    case AVRO_BYTES:
    case AVRO_FIXED:
    case AVRO_ARRAY:
    case AVRO_MAP:
    case AVRO_SYMBOLIC:
        return ValidatingGrammarGenerator::doGenerate(n, m);
    case AVRO_RECORD:
        {
            Production result;

            m.erase(n);

            size_t c = n->leaves();
            result.reserve(2 + 2 * c);
            result.push_back(Symbol::recordStartSymbol());
            for (size_t i = 0; i < c; ++i) {
                const NodePtr& leaf = n->leafAt(i);
                Production v = doGenerate(leaf, m);
                result.push_back(Symbol::fieldSymbol(n->nameAt(i)));
                copy(v.rbegin(), v.rend(), back_inserter(result));
            }
            result.push_back(Symbol::recordEndSymbol());
            reverse(result.begin(), result.end());

            bool found = m.find(n) != m.end();

            shared_ptr<Production> p = make_shared<Production>(result);
            m[n] = p;

            return found ? Production(1, Symbol::indirect(p)) : result;
        }
    case AVRO_ENUM:
        {
            vector<string> nn;
            size_t c = n->names();
            nn.reserve(c);
            for (int i = 0; i < c; ++i) {
                nn.push_back(n->nameAt(i));
            }
            Symbol r[] = {
                Symbol::nameListSymbol(nn),
                Symbol::enumSymbol() };
            Production result(r, r + 2);
            m[n] = boost::make_shared<Production>(result);
            return result;
        }
    case AVRO_UNION:
        {
            size_t c = n->leaves();

            vector<Production> vv;
            vv.reserve(c);

            vector<string> names;
            names.reserve(c);

            for (size_t i = 0; i < c; ++i) {
                const NodePtr& nn = n->leafAt(i);
                Production v = doGenerate(nn, m);
                if (nn->type() != AVRO_NULL) {
                    Production v2;
                    v2.push_back(Symbol::recordEndSymbol());
                    copy(v.begin(), v.end(), back_inserter(v2));
                    v.swap(v2);
                }
                vv.push_back(v);
                names.push_back(nameOf(nn));
            }
            Symbol r[] = {
                Symbol::alternative(vv),
                Symbol::nameListSymbol(names),
                Symbol::unionSymbol()
            };
            return Production(r, r + 3);
        }
    default:
        throw Exception("Unknown node type");
    }
}

static char toHex(unsigned int n) {
    return (n < 10) ? (n + '0') : (n + 'a' - 10);
}

class JsonParser : boost::noncopyable {
public:
    enum Token {
        tkNull,
        tkBool,
        tkLong,
        tkDouble,
        tkString,
        tkArrayStart,
        tkArrayEnd,
        tkObjectStart,
        tkObjectEnd
    };

private:
    enum State {
        stValue,    // Expect a data type
        stArray0,   // Expect a data type or ']'
        stArrayN,   // Expect a ',' or ']'
        stObject0,  // Expect a string or a '}'
        stObjectN,  // Expect a ',' or '}'
        stKey       // Expect a ':'
    };
    stack<State> stateStack;
    State curState;
    bool hasNext;
    char nextChar;
    bool peeked;

    StreamReader in_;
    Token curToken;
    bool bv;
    int64_t lv;
    double dv;
    string sv;

    Token doAdvance();
    Token tryLiteral(const char exp[], size_t n, Token tk);
    Token tryNumber(char ch);
    Token tryString();
    void unexpected(unsigned char ch);
    char next();

public:
    JsonParser() : curState(stValue), hasNext(false), peeked(false) { }

    void init(InputStream& is) {
        in_.reset(is);
    }

    Token advance() {
        if (! peeked) {
            curToken = doAdvance();
        } else {
            peeked = false;
        }
        return curToken;
    }

    Token peek() {
        if (! peeked) {
            curToken = doAdvance();
            peeked = true;
        }
        return curToken;
    }

    bool boolValue() {
        return bv;
    }

    Token cur() {
        return curToken;
    }

    double doubleValue() {
        return dv;
    }

    int64_t longValue() {
        return lv;
    }

    string stringValue() {
        return sv;
    }

    static const char* const tokenNames[];

    static const char* toString(Token tk) {
        return tokenNames[tk];
    }
};

const char* const
JsonParser::tokenNames[] = {
    "Null",
    "Bool",
    "Integer",
    "Double",
    "String",
    "Array start",
    "Array end",
    "Object start",
    "Object end",
};

char JsonParser::next()
{
    char ch = hasNext ? nextChar : ' ';
    while (isspace(ch)) {
        ch = in_.read();
    }
    hasNext = false;
    return ch;
}

JsonParser::Token JsonParser::doAdvance()
{
    char ch = next();
    if (ch == ']') {
        if (curState == stArray0 || stArrayN) {
            curState = stateStack.top();
            stateStack.pop();
            return tkArrayEnd;
        } else {
            unexpected(ch);
        }
    } else if (ch == '}') {
        if (curState == stObject0 || stObjectN) {
            curState = stateStack.top();
            stateStack.pop();
            return tkObjectEnd;
        } else {
            unexpected(ch);
        }
    } else if (ch == ',') {
        if (curState != stObjectN && curState != stArrayN) {
            unexpected(ch);
        }
        if (curState == stObjectN) {
            curState = stObject0;
        }
        ch = next();
    } else if (ch == ':') {
        if (curState != stKey) {
            unexpected(ch);
        }
        curState = stObjectN;
        ch = next();
    }

    if (curState == stObject0) {
        if (ch != '"') {
            unexpected(ch);
        }
        curState = stKey;
    } else if (curState == stArray0) {
        curState = stArrayN;
    }

    switch (ch) {
    case '[':
        stateStack.push(curState);
        curState = stArray0;
        return tkArrayStart;
    case '{':
        stateStack.push(curState);
        curState = stObject0;
        return tkObjectStart;
    case '"':
        return tryString();
    case 't':
        bv = true;
        return tryLiteral("rue", 3, tkBool);
    case 'f':
        bv = false;
        return tryLiteral("alse", 4, tkBool);
    case 'n':
        return tryLiteral("ull", 3, tkNull);
    default:
        if (isdigit(ch) || '-') {
            return tryNumber(ch);
        } else {
            unexpected(ch);
        }
    }
}

JsonParser::Token JsonParser::tryNumber(char ch)
{
    sv.clear();
    sv.push_back(ch);

    hasNext = false;
    int state = (ch == '-') ? 0 : (ch == 0) ? 1 : 2;
    for (; ;) {
        switch (state) {
        case 0:
            if (in_.hasMore()) {
                ch = in_.read();
                if (isdigit(ch)) {
                    state = (ch == 0) ? 1 : 2;
                    sv.push_back(ch);
                    continue;
                }
                hasNext = true;
            }
            break;
        case 1:
            if (in_.hasMore()) {
                ch = in_.read();
                if (ch == '.') {
                    state = 3;
                    sv.push_back(ch);
                    continue;
                }
                hasNext = true;
            }
            break;
        case 2:
            if (in_.hasMore()) {
                ch = in_.read();
                if (isdigit(ch)) {
                    sv.push_back(ch);
                    continue;
                } else if (ch == '.') {
                    state = 3;
                    sv.push_back(ch);
                    continue;
                }
                hasNext = true;
            }
            break;
        case 3:
        case 6:
            if (in_.hasMore()) {
                ch = in_.read();
                if (isdigit(ch)) {
                    sv.push_back(ch);
                    state++;
                    continue;
                }
                hasNext = true;
            }
            break;
        case 4:
            if (in_.hasMore()) {
                ch = in_.read();
                if (isdigit(ch)) {
                    sv.push_back(ch);
                    continue;
                } else if (ch == 'e' || ch == 'E') {
                    sv.push_back(ch);
                    state = 5;
                    continue;
                }
                hasNext = true;
            }
            break;
        case 5:
            if (in_.hasMore()) {
                in_.read();
                ch = next();
                if (ch == '+' || ch == '-') {
                    sv.push_back(ch);
                    state = 6;
                    continue;
                } else if (isdigit(ch)) {
                    sv.push_back(ch);
                    state = 7;
                    continue;
                }
                hasNext = true;
            }
            break;
        case 7:
            if (in_.hasMore()) {
                in_.read();
                ch = next();
                if (isdigit(ch)) {
                    sv.push_back(ch);
                    continue;
                }
                hasNext = true;
            }
            break;
        }
        if (state == 1 || state == 2 || state == 4 || state == 7) {
            if (hasNext) {
                nextChar = ch;
            }
            istringstream iss(sv);
            if (state == 1 || state == 2) {
                iss >> lv;
                return tkLong;
            } else {
                iss >> dv;
                return tkDouble;
            }
        } else {
            if (hasNext) {
                unexpected(ch);
            } else {
                throw Exception("Unexpected EOF");
            }
        }
    }
}

JsonParser::Token JsonParser::tryString()
{
    sv.clear();
    for ( ; ;) {
        char ch = in_.read();
        if (ch == '"') {
            return tkString;
        } else if (ch == '\\') {
            ch = in_.read();
            switch (ch) {
            case '"':
            case '\\':
            case '/':
                sv.push_back(ch);
                continue;
            case 'b':
                sv.push_back('\b');
                continue;
            case 'f':
                sv.push_back('\f');
                continue;
            case 'n':
                sv.push_back('\n');
                continue;
            case 'r':
                sv.push_back('\r');
                continue;
            case 't':
                sv.push_back('\t');
                continue;
            case 'U':
                {
                    unsigned int n = 0;
                    char e[4];
                    in_.readBytes(reinterpret_cast<uint8_t*>(e), 4);
                    for (int i = 0; i < 4; i++) {
                        n *= 16;
                        char c = e[i];
                        if (isdigit(c)) {
                            n += c - '0';
                        } else if (c >= 'a' && c <= 'f') {
                            n += c - 'a' + 10;
                        } else if (c >= 'A' && c <= 'F') {
                            n += c - 'A' + 10;
                        } else {
                            unexpected(c);
                        }
                    }
                    sv.push_back(n);
                }
                break;
            default:
                unexpected(ch);
            }
        } else {
            sv.push_back(ch);
        }
    }
}

void JsonParser::unexpected(unsigned char c)
{
    ostringstream oss;
    oss << "Unexpected character in json " << toHex(c / 16) << toHex(c % 16);
    throw Exception(oss.str());
}

JsonParser::Token JsonParser::tryLiteral(const char exp[], size_t n, Token tk)
{
    char c[100];
    in_.readBytes(reinterpret_cast<uint8_t*>(c), n);
    for (int i = 0; i < n; ++i) {
        if (c[i] != exp[i]) {
            unexpected(c[i]);
        }
    }
    if (in_.hasMore()) {
        nextChar = in_.read();
        if (isdigit(nextChar) || isalpha(nextChar)) {
            unexpected(nextChar);
        }
        hasNext = true;
    }
    return tk;
}

static void expectToken(JsonParser& in, JsonParser::Token tk)
{
    if (in.advance() != tk) {
        ostringstream oss;
        oss << "Incorrect token in the stream. Expected: "
            << JsonParser::toString(tk) << ", found "
            << JsonParser::toString(in.cur());
        throw Exception(oss.str());
    }
}

class JsonDecoderHandler {
    JsonParser& in_;
public:
    JsonDecoderHandler(JsonParser& p) : in_(p) { }
    size_t handle(const Symbol& s) {
        switch (s.kind()) {
        case Symbol::sRecordStart:
            expectToken(in_, JsonParser::tkObjectStart);
            break;
        case Symbol::sRecordEnd:
            expectToken(in_, JsonParser::tkObjectEnd);
            break;
        case Symbol::sField:
            expectToken(in_, JsonParser::tkString);
            if (s.extra<string>() != in_.stringValue()) {
                throw Exception("Incorrect field");
            }
            break;
        }
        return 0;
    }
};

template <typename P>
class JsonDecoder : public Decoder {
    JsonParser in_;
    JsonDecoderHandler handler_;
    P parser_;

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

    void expect(JsonParser::Token tk);
    void skipComposite();
public:

    JsonDecoder(const ValidSchema& s) :
        handler_(in_),
        parser_(JsonGrammarGenerator().generate(s), NULL, handler_) { }

};

template <typename P>
void JsonDecoder<P>::init(InputStream& is)
{
    in_.init(is);
}

template <typename P>
void JsonDecoder<P>::expect(JsonParser::Token tk)
{
    expectToken(in_, tk);
}

template <typename P>
void JsonDecoder<P>::decodeNull()
{
    parser_.advance(Symbol::sNull);
    expect(JsonParser::tkNull);
}

template <typename P>
bool JsonDecoder<P>::decodeBool()
{
    parser_.advance(Symbol::sBool);
    expect(JsonParser::tkBool);
    bool result = in_.boolValue();
    return result;
}

template <typename P>
int32_t JsonDecoder<P>::decodeInt()
{
    parser_.advance(Symbol::sInt);
    expect(JsonParser::tkLong);
    int64_t result = in_.longValue();
    if (result < INT32_MIN || result > INT32_MAX) {
        throw Exception(boost::format("Value out of range for Avro int: %1%")
            % result);
    }
    return static_cast<int32_t>(result);
}

template <typename P>
int64_t JsonDecoder<P>::decodeLong()
{
    parser_.advance(Symbol::sLong);
    expect(JsonParser::tkLong);
    int64_t result = in_.longValue();
    return result;
}

template <typename P>
float JsonDecoder<P>::decodeFloat()
{
    parser_.advance(Symbol::sFloat);
    expect(JsonParser::tkDouble);
    double result = in_.doubleValue();
    return static_cast<float>(result);
}

template <typename P>
double JsonDecoder<P>::decodeDouble()
{
    parser_.advance(Symbol::sDouble);
    expect(JsonParser::tkDouble);
    double result = in_.doubleValue();
    return result;
}

template <typename P>
void JsonDecoder<P>::decodeString(string& value)
{
    parser_.advance(Symbol::sString);
    expect(JsonParser::tkString);
    value = in_.stringValue();
}

template <typename P>
void JsonDecoder<P>::skipString()
{
    parser_.advance(Symbol::sString);
    expect(JsonParser::tkString);
}

static vector<uint8_t> toBytes(const string& s)
{
    return vector<uint8_t>(s.begin(), s.end());
}

template <typename P>
void JsonDecoder<P>::decodeBytes(vector<uint8_t>& value )
{
    parser_.advance(Symbol::sBytes);
    expect(JsonParser::tkString);
    value = toBytes(in_.stringValue());
}

template <typename P>
void JsonDecoder<P>::skipBytes()
{
    parser_.advance(Symbol::sBytes);
    expect(JsonParser::tkString);
}

template <typename P>
void JsonDecoder<P>::decodeFixed(size_t n, vector<uint8_t>& value)
{
    parser_.advance(Symbol::sFixed);
    parser_.assertSize(n);
    expect(JsonParser::tkString);
    value = toBytes(in_.stringValue());
    if (value.size() != n) {
        throw Exception("Incorrect value for fixed");
    }
}

template <typename P>
void JsonDecoder<P>::skipFixed(size_t n)
{
    parser_.advance(Symbol::sFixed);
    parser_.assertSize(n);
    expect(JsonParser::tkString);
    vector<uint8_t> result = toBytes(in_.stringValue());
    if (result.size() != n) {
        throw Exception("Incorrect value for fixed");
    }
}

template <typename P>
size_t JsonDecoder<P>::decodeEnum()
{
    parser_.advance(Symbol::sEnum);
    expect(JsonParser::tkString);
    size_t result = parser_.indexForName(in_.stringValue());
    return result;
}

template <typename P>
size_t JsonDecoder<P>::arrayStart()
{
    parser_.advance(Symbol::sArrayStart);
    expect(JsonParser::tkArrayStart);
    return arrayNext();
}

template <typename P>
size_t JsonDecoder<P>::arrayNext()
{
    parser_.processImplicitActions();
    if (in_.peek() == JsonParser::tkArrayEnd) {
        in_.advance();
        parser_.popRepeater();
        parser_.advance(Symbol::sArrayEnd);
        return 0;
    }
    parser_.setRepeatCount(1);
    return 1;
}

template<typename P>
void JsonDecoder<P>::skipComposite()
{
    size_t level = 0;
    for (; ;) {
        switch (in_.advance()) {
        case JsonParser::tkArrayStart:
        case JsonParser::tkObjectStart:
            ++level;
            continue;
        case JsonParser::tkArrayEnd:
        case JsonParser::tkObjectEnd:
            if (level == 0) {
                return;
            }
            --level;
            continue;
        default:
            continue;
        }
    }
}

template <typename P>
size_t JsonDecoder<P>::skipArray()
{
    parser_.advance(Symbol::sArrayStart);
    parser_.pop();
    parser_.advance(Symbol::sArrayEnd);
    expect(JsonParser::tkArrayStart);
    skipComposite();
    return 0;
}

template <typename P>
size_t JsonDecoder<P>::mapStart()
{
    parser_.advance(Symbol::sMapStart);
    expect(JsonParser::tkObjectStart);
    return mapNext();
}

template <typename P>
size_t JsonDecoder<P>::mapNext()
{
    parser_.processImplicitActions();
    if (in_.peek() == JsonParser::tkObjectEnd) {
        in_.advance();
        parser_.popRepeater();
        parser_.advance(Symbol::sMapEnd);
        return 0;
    }
    parser_.setRepeatCount(1);
    return 1;
}

template <typename P>
size_t JsonDecoder<P>::skipMap()
{
    parser_.advance(Symbol::sMapStart);
    parser_.pop();
    parser_.advance(Symbol::sMapEnd);
    expect(JsonParser::tkObjectStart);
    skipComposite();
    return 0;
}

template <typename P>
size_t JsonDecoder<P>::decodeUnionIndex()
{
    parser_.advance(Symbol::sUnion);

    size_t result;
    if (in_.peek() == JsonParser::tkNull) {
        result = parser_.indexForName("null");
    } else {
        expect(JsonParser::tkObjectStart);
        expect(JsonParser::tkString);
        result = parser_.indexForName(in_.stringValue());
    }
    parser_.selectBranch(result);
    return result;
}

class JsonGenerator {
    StreamWriter out_;
    enum State {
        stStart,
        stArray0,
        stArrayN,
        stMap0,
        stMapN,
        stKey,
    };

    stack<State> stateStack;
    State top;

    void write(const char *b, const char* p) {
        if (b != p) {
            out_.writeBytes(reinterpret_cast<const uint8_t*>(b), p - b);
        }
    }

    void escape(char c, const char* b, const char *p) {
        write(b, p);
        out_.write('\\');
        out_.write(c);
    }

    void escapeCtl(char c) {
        out_.write('\\');
        out_.write('U');
        out_.write('0');
        out_.write('0');
        out_.write(toHex((static_cast<unsigned char>(c)) / 16));
        out_.write(toHex((static_cast<unsigned char>(c)) % 16));
    }

    void doEncodeString(const std::string& s) {
        const char* b = &s[0];
        const char* e = b + s.size();
        out_.write('"');
        for (const char* p = b; p != e; p++) {
            switch (*p) {
            case '\\':
            case '"':
            case '/':
                escape(*p, b, p);
                break;
            case '\b':
                escape('b', b, p);
                break;
            case '\f':
                escape('f', b, p);
                break;
            case '\n':
                escape('n', b, p);
                break;
            case '\r':
                escape('r', b, p);
                break;
            case '\t':
                escape('t', b, p);
                break;
            default:
                if (! iscntrl(*p)) {
                    continue;
                }
                write(b, p);
                escapeCtl(*p);
                break;
            }
            b = p + 1;
        }
        write(b, e);
        out_.write('"');
    }

    void sep() {
        if (top == stArrayN) {
            out_.write(',');
        } else if (top == stArray0) {
            top = stArrayN;
        }
    }

    void sep2() {
        if (top == stKey) {
            top = stMapN;
        }
    }

public:
    JsonGenerator() : top(stStart) { }

    void init(OutputStream& os) {
        out_.reset(os);
    }

    void flush() {
        out_.flush();
    }

    void encodeNull() {
        sep();
        out_.writeBytes(reinterpret_cast<const uint8_t*>("null"), 4);
        sep2();
    }

    void encodeBool(bool b) {
        sep();
        if (b) {
            out_.writeBytes(reinterpret_cast<const uint8_t*>("true"), 4);
        } else {
            out_.writeBytes(reinterpret_cast<const uint8_t*>("false"), 5);
        }
        sep2();
    }

    template <typename T>
    void encodeNumber(T t) {
        sep();
        ostringstream oss;
        oss << t;
        const std::string& s = oss.str();
        out_.writeBytes(reinterpret_cast<const uint8_t*>(&s[0]), s.size());
        sep2();
    }

    void encodeString(const std::string& s) {
        if (top == stMap0) {
            top = stKey;
        } else if (top == stMapN) {
            out_.write(',');
            top = stKey;
        } else if (top == stKey) {
            top = stMapN;
        } else {
            sep();
        }
        doEncodeString(s);
        if (top == stKey) {
            out_.write(':');
        }
    }

    void encodeBinary(const uint8_t* bytes, size_t len) {
        sep();
        out_.write('"');
        const uint8_t* e = bytes + len;
        while (bytes != e) {
            escapeCtl(*bytes++);
        }
        out_.write('"');
        sep2();
    }

    void arrayStart() {
        sep();
        stateStack.push(top);
        top = stArray0;
        out_.write('[');
    }

    void arrayEnd() {
        top = stateStack.top();
        stateStack.pop();
        out_.write(']');
        sep2();
    }

    void objectStart() {
        sep();
        stateStack.push(top);
        top = stMap0;
        out_.write('{');
    }

    void objectEnd() {
        top = stateStack.top();
        stateStack.pop();
        out_.write('}');
        sep2();
    }

};

class JsonHandler {
    JsonGenerator& generator_;
public:
    JsonHandler(JsonGenerator& g) : generator_(g) { }
    size_t handle(const Symbol& s) {
        switch (s.kind()) {
        case Symbol::sRecordStart:
            generator_.objectStart();
            break;
        case Symbol::sRecordEnd:
            generator_.objectEnd();
            break;
        case Symbol::sField:
            generator_.encodeString(s.extra<string>());
            break;
        }
        return 0;
    }
};

template <typename P>
class JsonEncoder : public Encoder {
    JsonGenerator out_;
    JsonHandler handler_;
    P parser_;

    void init(OutputStream& os);
    void flush();
    void encodeNull();
    void encodeBool(bool b);
    void encodeInt(int32_t i);
    void encodeLong(int64_t l);
    void encodeFloat(float f);
    void encodeDouble(double d);
    void encodeString(const std::string& s);
    void encodeBytes(const uint8_t *bytes, size_t len);
    void encodeFixed(const uint8_t *bytes, size_t len);
    void encodeEnum(size_t e);
    void arrayStart();
    void arrayEnd();
    void mapStart();
    void mapEnd();
    void setItemCount(size_t count);
    void startItem();
    void encodeUnionIndex(size_t e);
public:
    JsonEncoder(const ValidSchema& schema) :
        handler_(out_),
        parser_(JsonGrammarGenerator().generate(schema), NULL, handler_) { }
};

template<typename P>
void JsonEncoder<P>::init(OutputStream& os)
{
    out_.init(os);
}

template<typename P>
void JsonEncoder<P>::flush()
{
    parser_.processImplicitActions();
    out_.flush();
}

template<typename P>
void JsonEncoder<P>::encodeNull()
{
    parser_.advance(Symbol::sNull);
    out_.encodeNull();
}

template<typename P>
void JsonEncoder<P>::encodeBool(bool b)
{
    parser_.advance(Symbol::sBool);
    out_.encodeBool(b);
}

template<typename P>
void JsonEncoder<P>::encodeInt(int32_t i)
{
    parser_.advance(Symbol::sInt);
    out_.encodeNumber(i);
}

template<typename P>
void JsonEncoder<P>::encodeLong(int64_t l)
{
    parser_.advance(Symbol::sLong);
    out_.encodeNumber(l);
}

template<typename P>
void JsonEncoder<P>::encodeFloat(float f)
{
    parser_.advance(Symbol::sFloat);
    out_.encodeNumber(f);
}

template<typename P>
void JsonEncoder<P>::encodeDouble(double d)
{
    parser_.advance(Symbol::sDouble);
    out_.encodeNumber(d);
}

template<typename P>
void JsonEncoder<P>::encodeString(const std::string& s)
{
    parser_.advance(Symbol::sString);
    out_.encodeString(s);
}

template<typename P>
void JsonEncoder<P>::encodeBytes(const uint8_t *bytes, size_t len)
{
    parser_.advance(Symbol::sBytes);
    out_.encodeBinary(bytes, len);
}

template<typename P>
void JsonEncoder<P>::encodeFixed(const uint8_t *bytes, size_t len)
{
    parser_.advance(Symbol::sFixed);
    parser_.assertSize(len);
    out_.encodeBinary(bytes, len);
}

template<typename P>
void JsonEncoder<P>::encodeEnum(size_t e)
{
    parser_.advance(Symbol::sEnum);
    const string& s = parser_.nameForIndex(e);
    out_.encodeString(s);
}

template<typename P>
void JsonEncoder<P>::arrayStart()
{
    parser_.advance(Symbol::sArrayStart);
    out_.arrayStart();
}

template<typename P>
void JsonEncoder<P>::arrayEnd()
{
    parser_.popRepeater();
    parser_.advance(Symbol::sArrayEnd);
    out_.arrayEnd();
}

template<typename P>
void JsonEncoder<P>::mapStart()
{
    parser_.advance(Symbol::sMapStart);
    out_.objectStart();
}

template<typename P>
void JsonEncoder<P>::mapEnd()
{
    parser_.popRepeater();
    parser_.advance(Symbol::sMapEnd);
    out_.objectEnd();
}

template<typename P>
void JsonEncoder<P>::setItemCount(size_t count)
{
    parser_.setRepeatCount(count);
}

template<typename P>
void JsonEncoder<P>::startItem()
{
    parser_.processImplicitActions();
    if (parser_.top() != Symbol::sRepeater) {
        throw Exception("startItem at not an item boundary");
    }
}

template<typename P>
void JsonEncoder<P>::encodeUnionIndex(size_t e)
{
    parser_.advance(Symbol::sUnion);

    const std::string name = parser_.nameForIndex(e);

    if (name != "null") {
        out_.objectStart();
        out_.encodeString(name);
    }
    parser_.selectBranch(e);
}

}   // namespace parsing

DecoderPtr jsonDecoder(const ValidSchema& s)
{
    return make_shared<parsing::JsonDecoder<
        parsing::SimpleParser<parsing::JsonDecoderHandler> > >(s);
}

EncoderPtr jsonEncoder(const ValidSchema& schema)
{
    return make_shared<parsing::JsonEncoder<
        parsing::SimpleParser<parsing::JsonHandler> > >(schema);
}

}   // namespace avro

