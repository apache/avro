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

#include "JsonIO.hh"

namespace avro {
namespace json {

using std::ostringstream;

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

void JsonParser::expectToken(Token tk)
{
    if (advance() != tk) {
        if (tk == tkDouble && cur() == tkString
            && (sv == "Infinity" || sv == "-Infinity" || sv == "NaN")) {
            curToken = tkDouble;
            dv = sv == "Infinity" ? std::numeric_limits<double>::infinity() :
                sv == "-Infinity" ? -std::numeric_limits<double>::infinity() :
                std::numeric_limits<double>::quiet_NaN();
            return;
        }
        ostringstream oss;
        oss << "Incorrect token in the stream. Expected: "
            << JsonParser::toString(tk) << ", found "
            << JsonParser::toString(cur());
        throw Exception(oss.str());
    }
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
            throw unexpected(ch);
        }
    } else if (ch == '}') {
        if (curState == stObject0 || stObjectN) {
            curState = stateStack.top();
            stateStack.pop();
            return tkObjectEnd;
        } else {
            throw unexpected(ch);
        }
    } else if (ch == ',') {
        if (curState != stObjectN && curState != stArrayN) {
            throw unexpected(ch);
        }
        if (curState == stObjectN) {
            curState = stObject0;
        }
        ch = next();
    } else if (ch == ':') {
        if (curState != stKey) {
            throw unexpected(ch);
        }
        curState = stObjectN;
        ch = next();
    }

    if (curState == stObject0) {
        if (ch != '"') {
            throw unexpected(ch);
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
        if (isdigit(ch) || ch == '-') {
            return tryNumber(ch);
        } else {
            throw unexpected(ch);
        }
    }
}

JsonParser::Token JsonParser::tryNumber(char ch)
{
    sv.clear();
    sv.push_back(ch);

    hasNext = false;
    int state = (ch == '-') ? 0 : (ch == '0') ? 1 : 2;
    for (; ;) {
        switch (state) {
        case 0:
            if (in_.hasMore()) {
                ch = in_.read();
                if (isdigit(ch)) {
                    state = (ch == '0') ? 1 : 2;
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
                ch = in_.read();
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
                ch = in_.read();
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
            std::istringstream iss(sv);
            if (state == 1 || state == 2) {
                iss >> lv;
                return tkLong;
            } else {
                iss >> dv;
                return tkDouble;
            }
        } else {
            if (hasNext) {
                throw unexpected(ch);
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
            case 'u':
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
                            throw unexpected(c);
                        }
                    }
                    sv.push_back(n);
                }
                break;
            default:
                throw unexpected(ch);
            }
        } else {
            sv.push_back(ch);
        }
    }
}

Exception JsonParser::unexpected(unsigned char c)
{
    std::ostringstream oss;
    oss << "Unexpected character in json " << toHex(c / 16) << toHex(c % 16);
    return Exception(oss.str());
}

JsonParser::Token JsonParser::tryLiteral(const char exp[], size_t n, Token tk)
{
    char c[100];
    in_.readBytes(reinterpret_cast<uint8_t*>(c), n);
    for (size_t i = 0; i < n; ++i) {
        if (c[i] != exp[i]) {
            throw unexpected(c[i]);
        }
    }
    if (in_.hasMore()) {
        nextChar = in_.read();
        if (isdigit(nextChar) || isalpha(nextChar)) {
            throw unexpected(nextChar);
        }
        hasNext = true;
    }
    return tk;
}

}
}

