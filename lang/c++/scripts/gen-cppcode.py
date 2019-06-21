#!/usr/bin/python

license = '''/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'''

headers = '''
#include <stdint.h>
#include <string>
#include <vector>
#include <map>
#include "Boost.hh"
#include "Exception.hh"
#include "AvroSerialize.hh"
#include "AvroParse.hh"
#include "Layout.hh"
'''

done = False

typeToC= { 'int' : 'int32_t', 'long' :'int64_t', 'float' : 'float', 'double' : 'double',
'boolean' : 'bool', 'null': 'avro::Null', 'string' : 'std::string', 'bytes' : 'std::vector<uint8_t>'}

structList = []
structNames = {}
forwardDeclareList = []

def addStruct(name, declaration) :
    if not structNames.has_key(name) :
        structNames[name] = True
        structList.append(declaration)

def addForwardDeclare(declaration) :
    code = 'struct ' + declaration + ';'
    forwardDeclareList.append(code)

def doPrimitive(type):
    return (typeToC[type], type)

def doSymbolic(args):
    addForwardDeclare(args[1])
    return (args[1], args[1])

def addLayout(name, type, var) :
    result = '        add(new $offsetType$(offset + offsetof($name$, $var$)));\n'
    result = result.replace('$name$', name)
    if typeToC.has_key(type) :
        offsetType = 'avro::PrimitiveLayout'
    else :
        offsetType = type+ '_Layout'
    result = result.replace('$offsetType$', offsetType)
    result = result.replace('$var$', var)
    return result;

def addSimpleLayout(type) :
    result = '        add(new $offsetType$);\n'
    if typeToC.has_key(type) :
        offsetType = 'avro::PrimitiveLayout'
    else :
        offsetType = type+ '_Layout'
    return result.replace('$offsetType$', offsetType)

recordfieldTemplate = '$type$ $name$\n'
recordTemplate = '''struct $name$ {

    $name$ () :
$initializers$
    { }

$recordfields$};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    s.writeRecord();
$serializefields$    s.writeRecordEnd();
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    p.readRecord();
$parsefields$    p.readRecordEnd();
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
$offsetlist$    }
};
'''

def doRecord(args):
    structDef = recordTemplate;
    typename = args[1];
    structDef = structDef.replace('$name$', typename);
    fields = ''
    serializefields = ''
    parsefields = ''
    initlist = ''
    offsetlist = ''
    end = False
    while not end:
        line = getNextLine()
        if line[0] == 'end':
            end = True
            initlist = initlist.rstrip(',\n')
        elif line[0] == 'name':
            fieldname = line[1]
            fieldline = getNextLine()
            fieldtypename, fieldtype = processType(fieldline)
            fields += '    ' +  fieldtypename + ' ' + fieldname + ';\n'
            serializefields += '    serialize(s, val.' + fieldname + ');\n'
            initlist += '        ' + fieldname + '(),\n'
            parsefields += '    parse(p, val.' + fieldname + ');\n'
            offsetlist += addLayout(typename, fieldtype, fieldname)
    structDef = structDef.replace('$initializers$', initlist)
    structDef = structDef.replace('$recordfields$', fields)
    structDef = structDef.replace('$serializefields$', serializefields)
    structDef = structDef.replace('$parsefields$', parsefields)
    structDef = structDef.replace('$offsetlist$', offsetlist)
    addStruct(typename, structDef)
    return (typename,typename)

uniontypestemplate = 'typedef $type$ Choice$N$Type'
unionTemplate = '''struct $name$ {

$typedeflist$
    typedef void* (*GenericSetter)($name$ *, int64_t);

    $name$() :
        choice(0),
        value(T0()),
        genericSetter(&$name$::genericSet)
    { }

$setfuncs$
#ifdef AVRO_BOOST_NO_ANYREF
    template<typename T>
    const T &getValue() const {
        const T *ptr = boost::any_cast<T>(&value);
        return *ptr;
    }
#else
    template<typename T>
    const T &getValue() const {
        return boost::any_cast<const T&>(value);
    }
#endif

    static void *genericSet($name$ *u, int64_t choice) {
        boost::any *val = &(u->value);
        void *data = NULL;
        switch (choice) {$switch$
        }
        return data;
    }

    int64_t choice;
    boost::any value;
    GenericSetter genericSetter;
};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    s.writeUnion(val.choice);
    switch(val.choice) {
$switchserialize$      default :
        throw avro::Exception("Unrecognized union choice");
    }
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    val.choice = p.readUnion();
    switch(val.choice) {
$switchparse$      default :
        throw avro::Exception("Unrecognized union choice");
    }
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, choice)));
        add(new avro::PrimitiveLayout(offset + offsetof($name$, genericSetter)));
$offsetlist$    }
};
'''

unionser = '      case $choice$:\n        serialize(s, val.getValue< $type$ >());\n        break;\n'
unionpar = '      case $choice$:\n        { $type$ chosenVal; parse(p, chosenVal); val.value = chosenVal; }\n        break;\n'

setfunc =  '''    void set_$name$(const $type$ &val) {
        choice = $N$;
        value =  val;
    };\n'''

switcher = '''\n          case $N$:
            *val = T$N$();
            data = boost::any_cast<T$N$>(val);
            break;'''


def doUnion(args):
    structDef = unionTemplate
    uniontypes = ''
    switchserialize= ''
    switchparse= ''
    typename = 'Union_of'
    setters = ''
    switches = ''
    offsetlist = ''
    i = 0
    end = False
    while not end:
        line = getNextLine()
        if line[0] == 'end': end = True
        else :
            uniontype, name = processType(line)
            typename += '_' + name
            uniontypes += '    ' + 'typedef ' + uniontype + ' T' + str(i) + ';\n'
            switch = unionser
            switch = switch.replace('$choice$', str(i))
            switch = switch.replace('$type$', uniontype)
            switchserialize += switch
            switch = unionpar
            switch = switch.replace('$choice$', str(i))
            switch = switch.replace('$type$', uniontype)
            switchparse += switch
            setter = setfunc
            setter = setter.replace('$name$', name)
            setter = setter.replace('$type$', uniontype)
            setter = setter.replace('$N$', str(i))
            setters += setter
            switch = switcher
            switches += switch.replace('$N$', str(i))
            offsetlist += addSimpleLayout(name)
        i+= 1
    structDef = structDef.replace('$name$', typename)
    structDef = structDef.replace('$typedeflist$', uniontypes)
    structDef = structDef.replace('$switchserialize$', switchserialize)
    structDef = structDef.replace('$switchparse$', switchparse)
    structDef = structDef.replace('$setfuncs$', setters)
    structDef = structDef.replace('$switch$', switches)
    structDef = structDef.replace('$offsetlist$', offsetlist)
    addStruct(typename, structDef)
    return (typename,typename)

enumTemplate = '''struct $name$ {

    enum EnumSymbols {
        $enumsymbols$
    };

    $name$() :
        value($firstsymbol$)
    { }

    EnumSymbols value;
};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    s.writeEnum(val.value);
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    val.value = static_cast<$name$::EnumSymbols>(p.readEnum());
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, value)));
    }
};
'''

def doEnum(args):
    structDef = enumTemplate;
    typename = args[1]
    structDef = structDef.replace('$name$', typename)
    end = False
    symbols = '';
    firstsymbol = '';
    while not end:
        line = getNextLine()
        if line[0] == 'end': end = True
        elif line[0] == 'name':
            if symbols== '' :
                firstsymbol = line[1]
            else :
                symbols += ', '
            symbols += line[1]
        else: print "error"
    structDef = structDef.replace('$enumsymbols$', symbols);
    structDef = structDef.replace('$firstsymbol$', firstsymbol);
    addStruct(typename, structDef)
    return (typename,typename)

arrayTemplate = '''struct $name$ {
    typedef $valuetype$ ValueType;
    typedef std::vector<ValueType> ArrayType;
    typedef ValueType* (*GenericSetter)($name$ *);

    $name$() :
        value(),
        genericSetter(&$name$::genericSet)
    { }

    static ValueType *genericSet($name$ *array) {
        array->value.push_back(ValueType());
        return &array->value.back();
    }

    void addValue(const ValueType &val) {
        value.push_back(val);
    }

    ArrayType value;
    GenericSetter genericSetter;

};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    const size_t size = val.value.size();
    if(size) {
        s.writeArrayBlock(size);
        for(size_t i = 0; i < size; ++i) {
            serialize(s, val.value[i]);
        }
    }
    s.writeArrayEnd();
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    val.value.clear();
    while(1) {
        int size = p.readArrayBlockSize();
        if(size > 0) {
            val.value.reserve(val.value.size() + size);
            while (size-- > 0) {
                val.value.push_back($name$::ValueType());
                parse(p, val.value.back());
            }
        }
        else {
            break;
        }
    }
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, genericSetter)));
$offsetlist$    }
};
'''

def doArray(args):
    structDef = arrayTemplate
    line = getNextLine()
    arraytype, typename = processType(line)
    offsetlist = addSimpleLayout(typename)
    typename = 'Array_of_' + typename

    structDef = structDef.replace('$name$', typename)
    structDef = structDef.replace('$valuetype$', arraytype)
    structDef = structDef.replace('$offsetlist$', offsetlist)

    line = getNextLine()
    if line[0] != 'end': print 'error'

    addStruct(typename, structDef)
    return (typename,typename)

mapTemplate = '''struct $name$ {
    typedef $valuetype$ ValueType;
    typedef std::map<std::string, ValueType> MapType;
    typedef ValueType* (*GenericSetter)($name$ *, const std::string &);

    $name$() :
        value(),
        genericSetter(&$name$::genericSet)
    { }

    void addValue(const std::string &key, const ValueType &val) {
        value.insert(MapType::value_type(key, val));
    }

    static ValueType *genericSet($name$ *map, const std::string &key) {
        map->value[key] = ValueType();
        return &(map->value[key]);
    }

    MapType value;
    GenericSetter genericSetter;

};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    if(val.value.size()) {
        s.writeMapBlock(val.value.size());
        $name$::MapType::const_iterator iter = val.value.begin();
        $name$::MapType::const_iterator end  = val.value.end();
        while(iter!=end) {
            serialize(s, iter->first);
            serialize(s, iter->second);
            ++iter;
        }
    }
    s.writeMapEnd();
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    val.value.clear();
    while(1) {
        int size = p.readMapBlockSize();
        if(size > 0) {
            while (size-- > 0) {
                std::string key;
                parse(p, key);
                $name$::ValueType m;
                parse(p, m);
                val.value.insert($name$::MapType::value_type(key, m));
            }
        }
        else {
            break;
        }
    }
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, genericSetter)));
$offsetlist$    }
};
'''

def doMap(args):
    structDef = mapTemplate
    line = getNextLine() # must be string
    line = getNextLine()
    maptype, typename = processType(line);

    offsetlist = addSimpleLayout(typename)
    typename = 'Map_of_' + typename

    structDef = structDef.replace('$name$', typename)
    structDef = structDef.replace('$valuetype$', maptype)
    structDef = structDef.replace('$offsetlist$', offsetlist)

    line = getNextLine()
    if line[0] != 'end': print 'error'
    addStruct(typename, structDef)
    return (typename,typename)

fixedTemplate = '''struct $name$ {
    enum {
        fixedSize = $N$
    };

    $name$() {
        memset(value, 0, sizeof(value));
    }

    uint8_t value[fixedSize];
};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    s.writeFixed(val.value);
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    p.readFixed(val.value);
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, value)));
    }
};
'''

def doFixed(args):
    structDef = fixedTemplate
    typename = args[1]
    size = args[2]

    line = getNextLine()
    if line[0] != 'end': print 'error'

    structDef = structDef.replace('$name$', typename)
    structDef = structDef.replace('$N$', size)
    addStruct(typename, structDef)
    return (typename,typename)

primitiveTemplate = '''struct $name$ {
    $type$ value;
};

template <typename Serializer>
inline void serialize(Serializer &s, const $name$ &val, const boost::true_type &) {
    s.writeValue(val.value);
}

template <typename Parser>
inline void parse(Parser &p, $name$ &val, const boost::true_type &) {
    p.readValue(val.value);
}

class $name$_Layout : public avro::CompoundLayout {
  public:
    $name$_Layout(size_t offset = 0) :
        CompoundLayout(offset)
    {
        add(new avro::PrimitiveLayout(offset + offsetof($name$, value)));
    }
};
'''

def doPrimitiveStruct(type):
    structDef = primitiveTemplate
    name =  type.capitalize()
    structDef = structDef.replace('$name$', name);
    structDef = structDef.replace('$type$', typeToC[type]);
    addStruct(name, structDef)

compoundBuilder= { 'record' : doRecord, 'union' : doUnion, 'enum' : doEnum,
'map' : doMap, 'array' : doArray, 'fixed' : doFixed, 'symbolic' : doSymbolic }

def processType(inputs) :
    type = inputs[0]
    if typeToC.has_key(type) :
        result = doPrimitive(type)
    else :
        func = compoundBuilder[type]
        result = func(inputs)
    return result

def generateCode() :
    inputs = getNextLine()
    type = inputs[0]
    if typeToC.has_key(type) :
        doPrimitiveStruct(type)
    else :
        func = compoundBuilder[type]
        func(inputs)

def getNextLine():
    try:
        line = raw_input()
    except:
        line = '';
        globals()["done"] = True

    if line == '':
        globals()["done"] = True
    return line.split(' ')

def writeHeader(filebase, namespace):
    headerstring = "%s_%s_hh__" % (namespace, filebase)

    print license
    print "#ifndef %s" % headerstring
    print "#define %s" % headerstring
    print headers
    print "namespace %s {\n" % namespace

    for x in forwardDeclareList:
        print "%s\n" % x

    for x in structList:
        print "/*----------------------------------------------------------------------------------*/\n"
        print "%s\n" % x

    print "\n} // namespace %s\n" % namespace

    print "namespace avro {\n"
    for x in structNames:
        print 'template <> struct is_serializable<%s::%s> : public boost::true_type{};' % (namespace, x)

    print "\n} // namespace avro\n"

    print "#endif // %s" % headerstring


def usage():
    print "-h, --help            print this helpful message"
    print "-i, --input=FILE      input file to read (default is stdin)"
    print "-o, --output=PATH     output file to generate (default is stdout)"
    print "-n, --namespace=LABEL namespace for schema (default is avrouser)"

if __name__ == "__main__":
    from sys import argv
    import getopt,sys

    try:
        opts, args = getopt.getopt(argv[1:], "hi:o:n:", ["help", "input=", "output=", "namespace="])

    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    namespace = 'avrouser'

    savein = sys.stdin
    saveout = sys.stdout
    inputFile = False
    outputFile = False
    outputFileBase = 'AvroGenerated'

    for o, a in opts:
        if o in ("-i", "--input"):
            try:
                inputFile = open(a, 'r')
                sys.stdin = inputFile
            except:
                print "Could not open file " + a
                sys.exit()
        elif o in ("-o", "--output"):
            try:
                outputFile = open(a, 'w')
                sys.stdout = outputFile
            except:
                print "Could not open file " + a
            outputFileBase = a.rstrip('.hp')  # strip for .h, .hh, .hpp
        elif o in ("-n", "--namespace"):
            namespace = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            print "Unhandled option: " + o
            usage()
            sys.exit()

    generateCode()
    writeHeader(outputFileBase, namespace)

    sys.stdin = savein
    sys.stdout = saveout
    if inputFile:
        inputFile.close()
    if outputFile:
        outputFile.close()

