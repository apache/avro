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
#include <sstream>

#include "Compiler.hh"
#include "Types.hh"
#include "Schema.hh"
#include "ValidSchema.hh"
#include "Stream.hh"

#include "json/JsonDom.hh"

using std::string;
using std::map;
using std::vector;
using std::pair;
using std::make_pair;

namespace avro {
using json::Entity;
using json::Object;
using json::Array;
using json::EntityType;

typedef map<Name, NodePtr> SymbolTable;


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

static NodePtr makeNode(const json::Entity& e, SymbolTable& st, const string& ns);

template <typename T>
concepts::SingleAttribute<T> asSingleAttribute(const T& t)
{
    concepts::SingleAttribute<T> n;
    n.add(t);
    return n;
}

static bool isFullName(const string& s)
{
    return s.find('.') != string::npos;
}
    
static Name getName(const string& name, const string& ns)
{
    return (isFullName(name)) ? Name(name) : Name(name, ns);
}

static NodePtr makeNode(const std::string& t, SymbolTable& st, const string& ns)
{
    NodePtr result = makePrimitive(t);
    if (result) {
        return result;
    }
    Name n = getName(t, ns);

    SymbolTable::const_iterator it = st.find(n);
    if (it != st.end()) {
        return NodePtr(new NodeSymbolic(asSingleAttribute(n), it->second));
    }
    throw Exception(boost::format("Unknown type: %1%") % n.fullname());
}

const json::Object::const_iterator findField(const Entity& e,
    const Object& m, const string& fieldName)
{
    Object::const_iterator it = m.find(fieldName);
    if (it == m.end()) {
        throw Exception(boost::format("Missing Json field \"%1%\": %2%") %
            fieldName % e.toString());
    } else {
        return it;
    }
}

template <typename T> void ensureType(const Entity& e, const string& name)
{
    if (e.type() != json::type_traits<T>::type()) {
        throw Exception(boost::format("Json field \"%1%\" is not a %2%: %3%") %
            name % json::type_traits<T>::name() % e.toString());
    }
}

const string& getStringField(const Entity& e, const Object& m,
                             const string& fieldName)
{
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<string>(it->second, fieldName);
    return it->second.stringValue();
}

const Array& getArrayField(const Entity& e, const Object& m,
                           const string& fieldName)
{
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<Array >(it->second, fieldName);
    return it->second.arrayValue();
}

const int64_t getLongField(const Entity& e, const Object& m,
                           const string& fieldName)
{
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<int64_t>(it->second, fieldName);
    return it->second.longValue();
}
    
struct Field {
    const string& name;
    const NodePtr schema;
    const GenericDatum defaultValue;
    Field(const string& n, const NodePtr& v, GenericDatum dv) :
        name(n), schema(v), defaultValue(dv) { }
};

static void assertType(const Entity& e, EntityType et)
{
    if (e.type() != et) {
        throw Exception(boost::format("Unexpected type for default value: "
            "Expected %1%, but found %2%") % et % e.type());
    }
}

static vector<uint8_t> toBin(const std::string& s)
{
    vector<uint8_t> result;
    result.resize(s.size());
    std::copy(s.c_str(), s.c_str() + s.size(), &result[0]);
    return result;
}

static string nameof(const NodePtr& n)
{
    Type t = n->type();
    switch (t) {
    case AVRO_STRING:
        return "string";
    case AVRO_BYTES:
        return "bytes";
    case AVRO_INT:
        return "int";
    case AVRO_LONG:
        return "long";
    case AVRO_FLOAT:
        return "float";
    case AVRO_DOUBLE:
        return "double";
    case AVRO_BOOL:
        return "boolean";
    case AVRO_NULL:
        return "null";
    case AVRO_RECORD:
    case AVRO_ENUM:
    case AVRO_FIXED:
    case AVRO_SYMBOLIC:
        return n->name().fullname();
    case AVRO_ARRAY:
        return "array";
    case AVRO_MAP:
        return "map";
    case AVRO_UNION:
        return "union";
    default:
        throw Exception(boost::format("Unknown type: %1%") % t);
    }
}

static GenericDatum makeGenericDatum(NodePtr n, const Entity& e,
    const SymbolTable& st)
{
    Type t = n->type();
    if (t == AVRO_SYMBOLIC) {
        n = st.find(n->name())->second;
        t = n->type();
    }
    switch (t) {
    case AVRO_STRING:
        assertType(e, json::etString);
        return GenericDatum(e.stringValue());
    case AVRO_BYTES:
        assertType(e, json::etString);
        return GenericDatum(toBin(e.stringValue()));
    case AVRO_INT:
        assertType(e, json::etLong);
        return GenericDatum(static_cast<int32_t>(e.longValue()));
    case AVRO_LONG:
        assertType(e, json::etLong);
        return GenericDatum(e.longValue());
    case AVRO_FLOAT:
        assertType(e, json::etDouble);
        return GenericDatum(static_cast<float>(e.doubleValue()));
    case AVRO_DOUBLE:
        assertType(e, json::etDouble);
        return GenericDatum(e.doubleValue());
    case AVRO_BOOL:
        assertType(e, json::etBool);
        return GenericDatum(e.boolValue());
    case AVRO_NULL:
        assertType(e, json::etNull);
        return GenericDatum();
    case AVRO_RECORD:
    {
        assertType(e, json::etObject);
        GenericRecord result(n);
        const map<string, Entity>& v = e.objectValue();
        for (size_t i = 0; i < n->leaves(); ++i) {
            map<string, Entity>::const_iterator it = v.find(n->nameAt(i));
            if (it == v.end()) {
                throw Exception(boost::format(
                    "No value found in default for %1%") % n->nameAt(i));
            }
            result.setFieldAt(i,
                makeGenericDatum(n->leafAt(i), it->second, st));
        }
        return GenericDatum(n, result);
    }
    case AVRO_ENUM:
        assertType(e, json::etString);
        return GenericDatum(n, GenericEnum(n, e.stringValue()));
    case AVRO_ARRAY:
    {
        assertType(e, json::etArray);
        GenericArray result(n);
        const vector<Entity>& elements = e.arrayValue();
        for (vector<Entity>::const_iterator it = elements.begin();
            it != elements.end(); ++it) {
            result.value().push_back(makeGenericDatum(n->leafAt(0), *it, st));
        }
        return GenericDatum(n, result);
    }
    case AVRO_MAP:
    {
        assertType(e, json::etObject);
        GenericMap result(n);
        const map<string, Entity>& v = e.objectValue();
        for (map<string, Entity>::const_iterator it = v.begin();
            it != v.end(); ++it) {
            result.value().push_back(make_pair(it->first,
                makeGenericDatum(n->leafAt(1), it->second, st)));
        }
        return GenericDatum(n, result);
    }
    case AVRO_UNION:
    {
        GenericUnion result(n);
        string name;
        Entity e2;
        if (e.type() == json::etNull) {
            name = "null";
            e2 = e;
        } else {
            assertType(e, json::etObject);
            const map<string, Entity>& v = e.objectValue();
            if (v.size() != 1) {
                throw Exception(boost::format("Default value for "
                    "union has more than one field: %1%") % e.toString());
            }
            map<string, Entity>::const_iterator it = v.begin();
            name = it->first;
            e2 = it->second;
        }
        for (size_t i = 0; i < n->leaves(); ++i) {
            const NodePtr& b = n->leafAt(i);
            if (nameof(b) == name) {
                result.selectBranch(i);
                result.datum() = makeGenericDatum(b, e2, st);
                return GenericDatum(n, result);
            }
        }
        throw Exception(boost::format("Invalid default value %1%") %
            e.toString());
    }
    case AVRO_FIXED:
        assertType(e, json::etString);
        return GenericDatum(n, GenericFixed(n, toBin(e.stringValue())));
    default:
        throw Exception(boost::format("Unknown type: %1%") % t);
    }
    return GenericDatum();
}


static Field makeField(const Entity& e, SymbolTable& st, const string& ns)
{
    const Object& m = e.objectValue();
    const string& n = getStringField(e, m, "name");
    Object::const_iterator it = findField(e, m, "type");
    map<string, Entity>::const_iterator it2 = m.find("default");
    NodePtr node = makeNode(it->second, st, ns);
    GenericDatum d = (it2 == m.end()) ? GenericDatum() :
        makeGenericDatum(node, it2->second, st);
    return Field(n, node, d);
}

static NodePtr makeRecordNode(const Entity& e,
    const Name& name, const Object& m, SymbolTable& st, const string& ns)
{        
    const Array& v = getArrayField(e, m, "fields");
    concepts::MultiAttribute<string> fieldNames;
    concepts::MultiAttribute<NodePtr> fieldValues;
    vector<GenericDatum> defaultValues;
    
    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
        Field f = makeField(*it, st, ns);
        fieldNames.add(f.name);
        fieldValues.add(f.schema);
        defaultValues.push_back(f.defaultValue);
    }
    return NodePtr(new NodeRecord(asSingleAttribute(name),
        fieldValues, fieldNames, defaultValues));
}

static NodePtr makeEnumNode(const Entity& e,
    const Name& name, const Object& m)
{
    const Array& v = getArrayField(e, m, "symbols");
    concepts::MultiAttribute<string> symbols;
    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
        if (it->type() != json::etString) {
            throw Exception(boost::format("Enum symbol not a string: %1%") %
                it->toString());
        }
        symbols.add(it->stringValue());
    }
    return NodePtr(new NodeEnum(asSingleAttribute(name), symbols));
}

static NodePtr makeFixedNode(const Entity& e,
    const Name& name, const Object& m)
{
    int v = static_cast<int>(getLongField(e, m, "size"));
    if (v <= 0) {
        throw Exception(boost::format("Size for fixed is not positive: ") %
            e.toString());
    }
    return NodePtr(new NodeFixed(asSingleAttribute(name),
        asSingleAttribute(v)));
}

static NodePtr makeArrayNode(const Entity& e, const Object& m,
    SymbolTable& st, const string& ns)
{
    Object::const_iterator it = findField(e, m, "items");
    return NodePtr(new NodeArray(asSingleAttribute(
        makeNode(it->second, st, ns))));
}

static NodePtr makeMapNode(const Entity& e, const Object& m,
    SymbolTable& st, const string& ns)
{
    Object::const_iterator it = findField(e, m, "values");

    return NodePtr(new NodeMap(asSingleAttribute(
        makeNode(it->second, st, ns))));
}

static Name getName(const Entity& e, const Object& m, const string& ns)
{
    const string& name = getStringField(e, m, "name");

    if (isFullName(name)) {
        return Name(name);
    } else {
        Object::const_iterator it = m.find("namespace");
        if (it != m.end()) {
            if (it->second.type() != json::type_traits<string>::type()) {
                throw Exception(boost::format(
                    "Json field \"%1%\" is not a %2%: %3%") %
                        "namespace" % json::type_traits<string>::name() %
                        it->second.toString());
            }
            Name result = Name(name, it->second.stringValue());
            return result;
        }
        return Name(name, ns);
    }
}

static NodePtr makeNode(const Entity& e, const Object& m,
    SymbolTable& st, const string& ns)
{
    const string& type = getStringField(e, m, "type");
    if (NodePtr result = makePrimitive(type)) {
        return result;
    } else if (type == "record" || type == "error" ||
        type == "enum" || type == "fixed") {
        Name nm = getName(e, m, ns);
        NodePtr result;
        if (type == "record" || type == "error") {
            result = NodePtr(new NodeRecord());
            st[nm] = result;
            NodePtr r = makeRecordNode(e, nm, m, st, nm.ns());
            (boost::dynamic_pointer_cast<NodeRecord>(r))->swap(
                *boost::dynamic_pointer_cast<NodeRecord>(result));
        } else {
            result = (type == "enum") ? makeEnumNode(e, nm, m) :
                makeFixedNode(e, nm, m);
            st[nm] = result;
        }
        return result;
    } else if (type == "array") {
        return makeArrayNode(e, m, st, ns);
    } else if (type == "map") {
        return makeMapNode(e, m, st, ns);
    }
    throw Exception(boost::format("Unknown type definition: %1%")
        % e.toString());
}

static NodePtr makeNode(const Entity& e, const Array& m,
    SymbolTable& st, const string& ns)
{
    concepts::MultiAttribute<NodePtr> mm;
    for (Array::const_iterator it = m.begin(); it != m.end(); ++it) {
        mm.add(makeNode(*it, st, ns));
    }
    return NodePtr(new NodeUnion(mm));
}

static NodePtr makeNode(const json::Entity& e, SymbolTable& st, const string& ns)
{
    switch (e.type()) {
    case json::etString:
        return makeNode(e.stringValue(), st, ns);
    case json::etObject:
        return makeNode(e, e.objectValue(), st, ns);
    case json::etArray:
        return makeNode(e, e.arrayValue(), st, ns);
    default:
        throw Exception(boost::format("Invalid Avro type: %1%") % e.toString());
    }
}

AVRO_DECL ValidSchema compileJsonSchemaFromStream(InputStream& is)
{
    json::Entity e = json::loadEntity(is);
    SymbolTable st;
    NodePtr n = makeNode(e, st, "");
    return ValidSchema(n);
}

AVRO_DECL ValidSchema compileJsonSchemaFromFile(const char* filename)
{
    std::auto_ptr<InputStream> s = fileInputStream(filename);
    return compileJsonSchemaFromStream(*s);
}

AVRO_DECL ValidSchema compileJsonSchemaFromMemory(const uint8_t* input, size_t len)
{
    return compileJsonSchemaFromStream(*memoryInputStream(input, len));
}

AVRO_DECL ValidSchema compileJsonSchemaFromString(const char* input)
{
    return compileJsonSchemaFromMemory(reinterpret_cast<const uint8_t*>(input),
        ::strlen(input));
}

AVRO_DECL ValidSchema compileJsonSchemaFromString(const std::string& input)
{
    return compileJsonSchemaFromMemory(
        reinterpret_cast<const uint8_t*>(&input[0]), input.size());
}

static ValidSchema compile(std::istream& is)
{
    std::auto_ptr<InputStream> in = istreamInputStream(is);
    return compileJsonSchemaFromStream(*in);
}

AVRO_DECL void compileJsonSchema(std::istream &is, ValidSchema &schema)
{
    if (!is.good()) {
        throw Exception("Input stream is not good");
    }

    schema = compile(is);
}

AVRO_DECL bool compileJsonSchema(std::istream &is, ValidSchema &schema, std::string &error)
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
