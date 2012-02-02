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

#include "Compiler.hh"
#include "Types.hh"
#include "Schema.hh"
#include "ValidSchema.hh"
#include "Stream.hh"

#include "json/JsonDom.hh"

extern void yyparse(void *ctx);

using std::string;
using std::map;
using std::vector;

namespace avro {

typedef map<string, NodePtr> SymbolTable;

using json::Entity;

// #define DEBUG_VERBOSE

static NodePtr makePrimitive(const std::string& t)
{
    if (t == "null") {
        return NodePtr(new NodePrimitive(AVRO_NULL));
    } else if (t == "boolean") {
        return NodePtr(new NodePrimitive(AVRO_BOOL));
    } else if (t == "int") {
        return NodePtr(new NodePrimitive(AVRO_INT));
    } else if (t == "long") {
        return NodePtr(new NodePrimitive(AVRO_LONG));
    } else if (t == "float") {
        return NodePtr(new NodePrimitive(AVRO_FLOAT));
    } else if (t == "double") {
        return NodePtr(new NodePrimitive(AVRO_DOUBLE));
    } else if (t == "string") {
        return NodePtr(new NodePrimitive(AVRO_STRING));
    } else if (t == "bytes") {
        return NodePtr(new NodePrimitive(AVRO_BYTES));
    } else {
        return NodePtr();
    }
}

static NodePtr makeNode(const json::Entity& e, SymbolTable& st);

template <typename T>
concepts::SingleAttribute<T> asSingleAttribute(const T& t)
{
    concepts::SingleAttribute<T> n;
    n.add(t);
    return n;
}

static NodePtr makeNode(const std::string& t, SymbolTable& st)
{
    NodePtr result = makePrimitive(t);
    if (result) {
        return result;
    }
    map<string, NodePtr>::const_iterator it = st.find(t);
    if (it != st.end()) {
        return NodePtr(new NodeSymbolic(asSingleAttribute(t), it->second));
    }
    throw Exception(boost::format("Unknown type: %1%") % t);
}

const map<string, Entity>::const_iterator findField(const Entity& e,
    const map<string, Entity>& m, const string& fieldName)
{
    map<string, Entity>::const_iterator it = m.find(fieldName);
    if (it == m.end()) {
        throw Exception(boost::format("Missing Json field \"%1%\": %2%") %
            fieldName % e.toString());
    } else {
        return it;
    }
}

template<typename T>
const T& getField(const Entity& e, const map<string, Entity>& m,
    const string& fieldName)
{
    map<string, Entity>::const_iterator it = findField(e, m, fieldName);
    if (it->second.type() != json::type_traits<T>::type()) {
        throw Exception(boost::format(
            "Json field \"%1%\" is not a %2%: %3%") %
                fieldName % json::type_traits<T>::name() %
                it->second.toString());
    } else {
        return it->second.value<T>();
    }
}

struct Field {
    const string& name;
    const NodePtr value;
    Field(const string& n, const NodePtr& v) : name(n), value(v) { }
};

static Field makeField(const Entity& e, SymbolTable& st)
{
    const map<string, Entity>& m = e.value<map<string, Entity> >();
    const string& n = getField<string>(e, m, "name");
    map<string, Entity>::const_iterator it = findField(e, m, "type");
    return Field(n, makeNode(it->second, st));
}

static NodePtr makeRecordNode(const Entity& e,
    const string& name, const map<string, Entity>& m, SymbolTable& st)
{
    const vector<Entity>& v = getField<vector<Entity> >(e, m, "fields");
    concepts::MultiAttribute<string> fieldNames;
    concepts::MultiAttribute<NodePtr> fieldValues;
    for (vector<Entity>::const_iterator it = v.begin(); it != v.end(); ++it) {
        Field f = makeField(*it, st);
        fieldNames.add(f.name);
        fieldValues.add(f.value);
    }
    return NodePtr(new NodeRecord(asSingleAttribute(name),
        fieldValues, fieldNames));
}

static NodePtr makeEnumNode(const Entity& e,
    const string& name, const map<string, Entity>& m)
{
    const vector<Entity>& v = getField<vector<Entity> >(e, m, "symbols");
    concepts::MultiAttribute<string> symbols;
    for (vector<Entity>::const_iterator it = v.begin(); it != v.end(); ++it) {
        if (it->type() != json::etString) {
            throw Exception(boost::format("Enum symbol not a string: %1%") %
                it->toString());
        }
        symbols.add(it->value<string>());
    }
    return NodePtr(new NodeEnum(asSingleAttribute(name), symbols));
}

static NodePtr makeFixedNode(const Entity& e,
    const string& name, const map<string, Entity>& m)
{
    int v = getField<int64_t>(e, m, "size");
    if (v <= 0) {
        throw Exception(boost::format("Size for fixed is not positive: ") %
            e.toString());
    }
    
    return NodePtr(new NodeFixed(asSingleAttribute(name),
        asSingleAttribute(v)));
}

static NodePtr makeArrayNode(const Entity& e, const map<string, Entity>& m,
    SymbolTable& st)
{
    map<string, Entity>::const_iterator it = findField(e, m, "items");
    return NodePtr(new NodeArray(asSingleAttribute(
        makeNode(it->second, st))));
}

static NodePtr makeMapNode(const Entity& e, const map<string, Entity>& m,
    SymbolTable& st)
{
    map<string, Entity>::const_iterator it = findField(e, m, "values");

    return NodePtr(new NodeMap(asSingleAttribute(
        makeNode(it->second, st))));
}

static NodePtr makeNode(const Entity& e, const map<string, Entity>& m,
    SymbolTable& st)
{
    const string& type = getField<string>(e, m, "type");
    if (NodePtr result = makePrimitive(type)) {
        if (m.size() > 1) {
            throw Exception(boost::format(
                "Unknown additional Json fields: %1%")
                % e.toString());
        } else {
            return result;
        }
    } else if (type == "record" || type == "error" ||
        type == "enum" || type == "fixed") {
        const string& name = getField<string>(e, m, "name");
        NodePtr result;
        if (type == "record" || type == "error") {
            result = NodePtr(new NodeRecord());
            st[name] = result;
            NodePtr r = makeRecordNode(e, name, m, st);
            (boost::dynamic_pointer_cast<NodeRecord>(r))->swap(
                *boost::dynamic_pointer_cast<NodeRecord>(result));
        } else {
            result = (type == "enum") ? makeEnumNode(e, name, m) :
                makeFixedNode(e, name, m);
            st[name] = result;
        }
        return result;
    } else if (type == "array") {
        return makeArrayNode(e, m, st);
    } else if (type == "map") {
        return makeMapNode(e, m, st);
    }
    throw Exception(boost::format("Unknown type definition: %1%")
        % e.toString());
}

static NodePtr makeNode(const Entity& e, const vector<Entity>& m,
    SymbolTable& st)
{
    concepts::MultiAttribute<NodePtr> mm;
    for (vector<Entity>::const_iterator it = m.begin(); it != m.end(); ++it) {
        mm.add(makeNode(*it, st));
    }
    return NodePtr(new NodeUnion(mm));
}

static NodePtr makeNode(const json::Entity& e, SymbolTable& st)
{
    switch (e.type()) {
    case json::etString:
        return makeNode(e.value<string>(), st);
    case json::etObject:
        return makeNode(e, e.value<map<string, Entity> >(), st);
    case json::etArray:
        return makeNode(e, e.value<vector<Entity> >(), st);
    default:
        throw Exception(boost::format("Invalid Avro type: %1%") % e.toString());
    }
}

ValidSchema compileJsonSchemaFromStream(InputStream& is)
{
    json::Entity e = json::loadEntity(is);
    SymbolTable st;
    NodePtr n = makeNode(e, st);
    return ValidSchema(n);
}

ValidSchema compileJsonSchemaFromMemory(const uint8_t* input, size_t len)
{
    return compileJsonSchemaFromStream(*memoryInputStream(input, len));
}

ValidSchema compileJsonSchemaFromString(const char* input)
{
    return compileJsonSchemaFromMemory(reinterpret_cast<const uint8_t*>(input),
        ::strlen(input));
}

static ValidSchema compile(std::istream& is)
{
    std::auto_ptr<InputStream> in = istreamInputStream(is);
    return compileJsonSchemaFromStream(*in);
}

void
compileJsonSchema(std::istream &is, ValidSchema &schema)
{
    if (!is.good()) {
        throw Exception("Input stream is not good");
    }

    schema = compile(is);
}

bool
compileJsonSchema(std::istream &is, ValidSchema &schema, std::string &error)
{
    try {
        compileJsonSchema(is, schema);
        return true;
    } catch (const Exception &e) {
        error = e.what();
        return false;
    }

}

} // namespace avro
