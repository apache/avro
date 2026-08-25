// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var util = require('util');

var WARNING = 'Validator API is deprecated. Please use the type API instead.';
Validator = util.deprecate(Validator, WARNING);
ProtocolValidator = util.deprecate(ProtocolValidator, WARNING);

var objectToString = Object.prototype.toString;


function isObject(obj) {
  var type = typeof obj;
  return type === 'function' || (type === 'object' && !!obj);
}

function isString(obj) {
  return objectToString.call(obj) === '[object String]';
}

function isNumber(obj) {
  return objectToString.call(obj) === '[object Number]';
}

function isBoolean(obj) {
  return obj === true ||
    obj === false ||
    objectToString.call(obj) === '[object Boolean]';
}

var AvroSpec = {
  PrimitiveTypes: ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'],
  ComplexTypes: ['record', 'enum', 'array', 'map', 'union', 'fixed']
};
AvroSpec.Types = AvroSpec.PrimitiveTypes.concat(AvroSpec.ComplexTypes);

var InvalidSchemaError = function(msg) { return new Error('InvalidSchemaError: ' + msg); };
var InvalidProtocolError = function(msg) { return new Error('InvalidProtocolError: ' + msg); };
var ValidationError = function(msg) { return new Error('ValidationError: ' + msg); };
var ProtocolValidationError = function(msg) { return new Error('ProtocolValidationError: ' + msg); };


function Record(name, namespace, fields) {
  function validateArgs(name, namespace, fields) {
    if (!isString(name)) {
      throw new InvalidSchemaError('Record name must be string');
    }

    if (namespace != null && !isString(namespace)) {
      throw new InvalidSchemaError('Record namespace must be string or null');
    }

    if (!Array.isArray(fields)) {
      throw new InvalidSchemaError('Record name must be string');
    }
  }

  validateArgs(name, namespace, fields);

  this.name = name;
  this.namespace = namespace;
  this.fields = fields;
}

function makeFullyQualifiedTypeName(schema, namespace) {
  var typeName = null;
  if (isString(schema)) {
    typeName = schema;
  } else if (isObject(schema)) {
    if (isString(schema.namespace)) {
      namespace = schema.namespace;
    }
    if (isString(schema.name)) {
      typeName = schema.name;
    } else if (isString(schema.type)) {
      typeName = schema.type;
    }
  } else {
    throw new InvalidSchemaError('unable to determine fully qualified type name from schema ' + JSON.stringify(schema) + ' in namespace ' + namespace);
  }

  if (!isString(typeName)) {
    throw new InvalidSchemaError('unable to determine type name from schema ' + JSON.stringify(schema) + ' in namespace ' + namespace);
  }

  if (typeName.indexOf('.') !== -1) {
    return typeName;
  } else if (AvroSpec.PrimitiveTypes.includes(typeName)) {
    return typeName;
  } else if (isString(namespace)) {
    return namespace + '.' + typeName;
  } else {
    return typeName;
  }
}

function Union(typeSchemas, namespace) {
  this.branchNames = function() {
    return typeSchemas.map(function(typeSchema) { return makeFullyQualifiedTypeName(typeSchema, namespace); });
  };

  function validateArgs(typeSchemas) {
    if (!Array.isArray(typeSchemas) || typeSchemas.length === 0) {
      throw new InvalidSchemaError('Union must have at least 1 branch');
    }
  }

  validateArgs(typeSchemas);

  this.typeSchemas = typeSchemas;
  this.namespace = namespace;
}

function Enum(symbols) {

  function validateArgs(symbols) {
    if (!Array.isArray(symbols)) {
      throw new InvalidSchemaError('Enum must have array of symbols, got ' + JSON.stringify(symbols));
    }
    if (!symbols.every(function(symbol) { return isString(symbol); })) {
      throw new InvalidSchemaError('Enum symbols must be strings, got ' + JSON.stringify(symbols));
    }
  }

  validateArgs(symbols);

  this.symbols = symbols;
}

function AvroArray(itemSchema) {

  function validateArgs(itemSchema) {
    if (itemSchema == null) {
      throw new InvalidSchemaError('Array "items" schema should not be null or undefined');
    }
  }

  validateArgs(itemSchema);

  this.itemSchema = itemSchema;
}

function Map(valueSchema) {

  function validateArgs(valueSchema) {
    if (valueSchema == null) {
      throw new InvalidSchemaError('Map "values" schema should not be null or undefined');
    }
  }

  validateArgs(valueSchema);

  this.valueSchema = valueSchema;
}

function Field(name, schema) {
  function validateArgs(name, schema) {
    if (!isString(name)) {
      throw new InvalidSchemaError('Field name must be string');
    }
  }

  this.name = name;
  this.schema = schema;
}

function Primitive(type) {
  function validateArgs(type) {
    if (!isString(type)) {
      throw new InvalidSchemaError('Primitive type name must be a string');
    }

    if (!AvroSpec.PrimitiveTypes.includes(type)) {
      throw new InvalidSchemaError('Primitive type must be one of: ' + JSON.stringify(AvroSpec.PrimitiveTypes) + '; got ' + type);
    }
  }

  validateArgs(type);

  this.type = type;
}

function Validator(schema, namespace, namedTypes) {
  this.validate = function(obj) {
    return _validate(this.schema, obj);
  };

  var _validate = function(schema, obj) {
    if (schema instanceof Record) {
      return _validateRecord(schema, obj);
    } else if (schema instanceof Union) {
      return _validateUnion(schema, obj);
    } else if (schema instanceof Enum) {
      return _validateEnum(schema, obj);
    } else if (schema instanceof AvroArray) {
      return _validateArray(schema, obj);
    } else if (schema instanceof Map) {
      return _validateMap(schema, obj);
    } else if (schema instanceof Primitive) {
      return _validatePrimitive(schema, obj);
    } else {
      throw new InvalidSchemaError('validation not yet implemented: ' + JSON.stringify(schema));
    }
  };

  var _validateRecord = function(schema, obj) {
    if (!isObject(obj) || Array.isArray(obj)) {
      throw new ValidationError('Expected record Javascript type to be non-array object, got ' + JSON.stringify(obj));
    }

    var schemaFieldNames = schema.fields.map(function(field) { return field.name; }).sort();
    var objFieldNames = Object.keys(obj).sort();
    if (!util.isDeepStrictEqual(schemaFieldNames, objFieldNames)) {
      throw new ValidationError('Expected record fields ' + JSON.stringify(schemaFieldNames) + '; got ' + JSON.stringify(objFieldNames));
    }

    return schema.fields.every(function(field) {
      return _validate(field.schema, obj[field.name]);
    });
  };

  var _validateUnion = function(schema, obj) {
    if (isObject(obj)) {
      if (Array.isArray(obj)) {
        throw new ValidationError('Expected union Javascript type to be non-array object (or null), got ' + JSON.stringify(obj));
      } else if (Object.keys(obj).length !== 1) {
        throw new ValidationError('Expected union Javascript object to be object with exactly 1 key (or null), got ' + JSON.stringify(obj));
      } else {
        var unionBranch = Object.keys(obj)[0];
        if (unionBranch === "") {
          throw new ValidationError('Expected union Javascript object to contain non-empty string branch, got ' + JSON.stringify(obj));
        }
        if (schema.branchNames().includes(unionBranch)) {
          return true;
        } else {
          throw new ValidationError('Expected union branch to be one of ' + JSON.stringify(schema.branchNames()) + '; got ' + JSON.stringify(unionBranch));
        }
      }
    } else if (obj === null) {
      if (schema.branchNames().includes('null')) {
        return true;
      } else {
        throw new ValidationError('Expected union branch to be one of ' + JSON.stringify(schema.branchNames()) + '; got ' + JSON.stringify(obj));
      }
    } else {
      throw new ValidationError('Expected union Javascript object to be non-array object of size 1 or null, got ' + JSON.stringify(obj));
    }
  };

  var _validateEnum = function(schema, obj) {
    if (isString(obj)) {
      if (schema.symbols.includes(obj)) {
        return true;
      } else {
        throw new ValidationError('Expected enum value to be one of ' + JSON.stringify(schema.symbols) + '; got ' + JSON.stringify(obj));
      }
    } else {
      throw new ValidationError('Expected enum Javascript object to be string, got ' + JSON.stringify(obj));
    }
  };

  var _validateArray = function(schema, obj) {
    if (Array.isArray(obj)) {
      return obj.every(function(member) { return _validate(schema.itemSchema, member); });
    } else {
      throw new ValidationError('Expected array Javascript object to be array, got ' + JSON.stringify(obj));
    }
  };

  var _validateMap = function(schema, obj) {
    if (isObject(obj) && !Array.isArray(obj)) {
      return Object.keys(obj).every(function(key) {
        return _validate(schema.valueSchema, obj[key]);
      });
    } else if (Array.isArray(obj)) {
      throw new ValidationError('Expected map Javascript object to be non-array object, got array ' + JSON.stringify(obj));
    } else {
      throw new ValidationError('Expected map Javascript object to be non-array object, got ' + JSON.stringify(obj));
    }
  };

  var _validatePrimitive = function(schema, obj) {
    switch (schema.type) {
      case 'null':
        if (obj == null) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript null or undefined for Avro null, got ' + JSON.stringify(obj));
        }
        break;
      case 'boolean':
        if (isBoolean(obj)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript boolean for Avro boolean, got ' + JSON.stringify(obj));
        }
        break;
      case 'int':
        if (isNumber(obj) && Math.floor(obj) === obj && Math.abs(obj) <= Math.pow(2, 31)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript int32 number for Avro int, got ' + JSON.stringify(obj));
        }
        break;
      case 'long':
        if (isNumber(obj) && Math.floor(obj) === obj && Math.abs(obj) <= Math.pow(2, 63)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript int64 number for Avro long, got ' + JSON.stringify(obj));
        }
        break;
      case 'float':
        if (isNumber(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript float number for Avro float, got ' + JSON.stringify(obj));
        }
        break;
      case 'double':
        if (isNumber(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript double number for Avro double, got ' + JSON.stringify(obj));
        }
        break;
      case 'bytes':
        throw new InvalidSchemaError('not yet implemented: ' + schema.type);
      case 'string':
        if (isString(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript string for Avro string, got ' + JSON.stringify(obj));
        }
        break;
      default:
        throw new InvalidSchemaError('unrecognized primitive type: ' + schema.type);
    }
  };

  // TODO: namespace handling is rudimentary. multiple namespaces within a certain nested schema definition
  // are probably buggy.
  var _namedTypes = namedTypes || {};
  var _saveNamedType = function(fullyQualifiedTypeName, schema) {
    if (Object.hasOwn(_namedTypes, fullyQualifiedTypeName)) {
      if (!util.isDeepStrictEqual(_namedTypes[fullyQualifiedTypeName], schema)) {
        throw new InvalidSchemaError('conflicting definitions for type ' + fullyQualifiedTypeName + ': ' + JSON.stringify(_namedTypes[fullyQualifiedTypeName]) + ' and ' + JSON.stringify(schema));
      }
    } else {
      _namedTypes[fullyQualifiedTypeName] = schema;
    }
  };

  var _lookupTypeByFullyQualifiedName = function(fullyQualifiedTypeName) {
    if (Object.hasOwn(_namedTypes, fullyQualifiedTypeName)) {
      return _namedTypes[fullyQualifiedTypeName];
    } else {
      return null;
    }
  };

  var _parseNamedType = function(schema, namespace) {
    if (AvroSpec.PrimitiveTypes.includes(schema)) {
      return new Primitive(schema);
    } else if (_lookupTypeByFullyQualifiedName(makeFullyQualifiedTypeName(schema, namespace)) !== null) {
      return _lookupTypeByFullyQualifiedName(makeFullyQualifiedTypeName(schema, namespace));
    } else {
      throw new InvalidSchemaError('unknown type name: ' + JSON.stringify(schema) + '; known type names are ' + JSON.stringify(Object.keys(_namedTypes)));
    }
  };

  var _parseSchema = function(schema, parentSchema, namespace) {
    if (schema == null) {
      throw new InvalidSchemaError('schema is null, in parentSchema: ' + JSON.stringify(parentSchema));
    } else if (isString(schema)) {
      return _parseNamedType(schema, namespace);
    } else if (isObject(schema) && !Array.isArray(schema)) {
      if (schema.type === 'record') {
        var newRecord = new Record(schema.name, schema.namespace, schema.fields.map(function(field) {
          return new Field(field.name, _parseSchema(field.type, schema, schema.namespace || namespace));
        }));
        _saveNamedType(makeFullyQualifiedTypeName(schema, namespace), newRecord);
        return newRecord;
      } else if (schema.type === 'enum') {
        if (Object.hasOwn(schema, 'symbols')) {
          var newEnum = new Enum(schema.symbols);
          _saveNamedType(makeFullyQualifiedTypeName(schema, namespace), newEnum);
          return newEnum;
        } else {
          throw new InvalidSchemaError('enum must specify symbols, got ' + JSON.stringify(schema));
        }
      } else if (schema.type === 'array') {
        if (Object.hasOwn(schema, 'items')) {
          return new AvroArray(_parseSchema(schema.items, schema, namespace));
        } else {
          throw new InvalidSchemaError('array must specify "items" schema, got ' + JSON.stringify(schema));
        }
      } else if (schema.type === 'map') {
        if (Object.hasOwn(schema, 'values')) {
          return new Map(_parseSchema(schema.values, schema, namespace));
        } else {
          throw new InvalidSchemaError('map must specify "values" schema, got ' + JSON.stringify(schema));
        }
      } else if (Object.hasOwn(schema, 'type') && AvroSpec.PrimitiveTypes.includes(schema.type)) {
        return _parseNamedType(schema.type, namespace);
      } else {
        throw new InvalidSchemaError('not yet implemented: ' + schema.type);
      }
    } else if (Array.isArray(schema)) {
      if (schema.length === 0) {
        throw new InvalidSchemaError('unions must have at least 1 branch');
      }
      var branchTypes = schema.map(function(branchType) { return _parseSchema(branchType, schema, namespace); });
      return new Union(branchTypes, namespace);
    } else {
      throw new InvalidSchemaError('unexpected Javascript type for schema: ' + (typeof schema));
    }
  };

  this.rawSchema = schema;
  this.schema = _parseSchema(schema, null, namespace);
}

Validator.validate = function(schema, obj) {
  return (new Validator(schema)).validate(obj);
}

function ProtocolValidator(protocol) {
  this.validate = function(typeName, obj) {
    var fullyQualifiedTypeName = makeFullyQualifiedTypeName(typeName, protocol.namespace);
    if (!Object.hasOwn(_typeSchemaValidators, fullyQualifiedTypeName)) {
      throw new ProtocolValidationError('Protocol does not contain definition for type ' + JSON.stringify(fullyQualifiedTypeName) + ' (fully qualified from input "' + typeName + '"); known types are ' + JSON.stringify(Object.keys(_typeSchemaValidators)));
    }
    return _typeSchemaValidators[fullyQualifiedTypeName].validate(obj);
  };

  var _typeSchemaValidators = {};
  var _initSchemaValidators = function(protocol) {
    var namedTypes = {};
    if (protocol == null || !Object.hasOwn(protocol, 'protocol') || !isString(protocol.protocol)) {
      throw new InvalidProtocolError('Protocol must contain a "protocol" attribute with a string value');
    }
    if (Array.isArray(protocol.types)) {
      protocol.types.forEach(function(typeSchema) {
        var schemaValidator = new Validator(typeSchema, protocol.namespace, namedTypes);
        var fullyQualifiedTypeName = makeFullyQualifiedTypeName(typeSchema, protocol.namespace);
        _typeSchemaValidators[fullyQualifiedTypeName] = schemaValidator;
      });
    }
  };

  _initSchemaValidators(protocol);
}

ProtocolValidator.validate = function(protocol, typeName, obj) {
  return (new ProtocolValidator(protocol)).validate(typeName, obj);
};

if (typeof exports !== 'undefined') {
  exports['Validator'] = Validator;
  exports['ProtocolValidator'] = ProtocolValidator;
}
