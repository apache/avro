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

#ifndef avro_json_JsonIO_hh__
#define avro_json_JsonIO_hh__

#include <stack>
#include <string>
#include <sstream>
#include <boost/utility.hpp>

#include "Config.hh"
#include "Stream.hh"

namespace avro {
namespace json {

inline char toHex(unsigned int n) {
    return (n < 10) ? (n + '0') : (n + 'a' - 10);
}


class AVRO_DECL JsonParser : boost::noncopyable {
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
    std::stack<State> stateStack;
    State curState;
    bool hasNext;
    char nextChar;
    bool peeked;

    StreamReader in_;
    Token curToken;
    bool bv;
    int64_t lv;
    double dv;
    std::string sv;

    Token doAdvance();
    Token tryLiteral(const char exp[], size_t n, Token tk);
    Token tryNumber(char ch);
    Token tryString();
    Exception unexpected(unsigned char ch);
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

    void expectToken(Token tk);

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

    std::string stringValue() {
        return sv;
    }

    static const char* const tokenNames[];

    static const char* toString(Token tk) {
        return tokenNames[tk];
    }
};

class AVRO_DECL JsonGenerator {
    StreamWriter out_;
    enum State {
        stStart,
        stArray0,
        stArrayN,
        stMap0,
        stMapN,
        stKey,
    };

    std::stack<State> stateStack;
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
        std::ostringstream oss;
        oss << t;
        const std::string& s = oss.str();
        out_.writeBytes(reinterpret_cast<const uint8_t*>(&s[0]), s.size());
        sep2();
    }

    void encodeNumber(double t);

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

}
}

#endif
