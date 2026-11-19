/* jshint node: true, mocha: true */

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

'use strict';

var validator = require('../etc/deprecated/validator'),
    Validator = validator.Validator,
    ProtocolValidator = validator.ProtocolValidator,
    assert = require('assert');

describe('deprecated validator', function () {
  it('nonexistent/null/undefined', function () {
    assert.throws(function() { return new Validator(); });
    assert.throws(function() { return new Validator(null); });
    assert.throws(function() { return new Validator(undefined); });
  });
  it('unrecognized primitive type name', function () {
    assert.throws(function() { return new Validator('badtype'); });
  });
  it('invalid schema javascript type', function () {
    assert.throws(function() { return new Validator(123); });
    assert.throws(function() { return new Validator(function() { }); });
  });

  // Primitive types
  it('null', function () {
    assert(Validator.validate('null', null));
    assert(Validator.validate('null', undefined));
    assert.throws(function() { Validator.validate('null', 1); });
    assert.throws(function() { Validator.validate('null', 'a'); });
  });
  it('boolean', function () {
    assert(Validator.validate('boolean', true));
    assert(Validator.validate('boolean', false));
    assert.throws(function() { Validator.validate('boolean', null); });
    assert.throws(function() { Validator.validate('boolean', 1); });
    assert.throws(function() { Validator.validate('boolean', 'a'); });
  });
  it('int', function () {
    assert(Validator.validate('int', 1));
    assert(Validator.validate('long', Math.pow(2, 31) - 1));
    assert.throws(function() { Validator.validate('int', 1.5); });
    assert.throws(function() { Validator.validate('int', Math.pow(2, 40)); });
    assert.throws(function() { Validator.validate('int', null); });
    assert.throws(function() { Validator.validate('int', 'a'); });
  });
  it('long', function () {
    assert(Validator.validate('long', 1));
    assert(Validator.validate('long', Math.pow(2, 63) - 1));
    assert.throws(function() { Validator.validate('long', 1.5); });
    assert.throws(function() { Validator.validate('long', Math.pow(2, 70)); });
    assert.throws(function() { Validator.validate('long', null); });
    assert.throws(function() { Validator.validate('long', 'a'); });
  });
  it('float', function () {
    assert(Validator.validate('float', 1));
    assert(Validator.validate('float', 1.5));
    assert.throws(function() { Validator.validate('float', 'a'); });
    assert.throws(function() { Validator.validate('float', null); });
  });
  it('double', function () {
    assert(Validator.validate('double', 1));
    assert(Validator.validate('double', 1.5));
    assert.throws(function() { Validator.validate('double', 'a'); });
    assert.throws(function() { Validator.validate('double', null); });
  });
  it('bytes', function () {
    // not implemented yet
    assert.throws(function() { Validator.validate('bytes', 1); });
  });
  it('string', function () {
    assert(Validator.validate('string', 'a'));
    assert.throws(function() { Validator.validate('string', 1); });
    assert.throws(function() { Validator.validate('string', null); });
  });
  it('boxed primitive values', function () {
    assert(Validator.validate('string', Object('a')));
    assert(Validator.validate('boolean', Object(true)));
    assert(Validator.validate('float', Object(1)));
    assert.throws(function() { Validator.validate('int', Object(1)); });
  });
  it('special number values', function () {
    assert(Validator.validate('float', NaN));
    assert.throws(function() { Validator.validate('int', NaN); });
  });

  // Records
  it('empty-record', function () {
    var schema = {type: 'record', name: 'EmptyRecord', fields: []};
    assert(Validator.validate(schema, {}));
    assert(Validator.validate(schema, function () {}));
    assert.throws(function() { Validator.validate(schema, 1); });
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, 'a'); });
  });
  it('record-with-string', function () {
    var schema = {type: 'record', name: 'EmptyRecord', fields: [{name: 'stringField', type: 'string'}]};
    assert(Validator.validate(schema, {stringField: 'a'}));
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, {stringField: 1}); });
    assert.throws(function() { Validator.validate(schema, {stringField: []}); });
    assert.throws(function() { Validator.validate(schema, {stringField: {}}); });
    assert.throws(function() { Validator.validate(schema, {stringField: null}); });
    assert.throws(function() { Validator.validate(schema, {stringField: 'a', unexpectedField: 'a'}); });
  });
  it('record-with-string-and-number', function () {
    var schema = {type: 'record', name: 'EmptyRecord', fields: [{name: 'stringField', type: 'string'}, {name: 'intField', type: 'int'}]};
    assert(Validator.validate(schema, {stringField: 'a', intField: 1}));
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, {stringField: 'a'}); });
    assert.throws(function() { Validator.validate(schema, {intField: 1}); });
    assert.throws(function() { Validator.validate(schema, {stringField: 'a', intField: 1, unexpectedField: 'a'}); });
  });
  it('nested-record-with-namespace-relative', function () {
    var schema = {type: 'record', namespace: 'x.y.z', name: 'RecordA', fields: [{name: 'recordBField1', type: ['null', {type: 'record', name: 'RecordB', fields: []}]}, {name: 'recordBField2', type: 'RecordB'}]};
    assert(Validator.validate(schema, {recordBField1: null, recordBField2: {}}));
    assert(Validator.validate(schema, {recordBField1: {'x.y.z.RecordB': {}}, recordBField2: {}}));
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, {recordBField1: null}); });
    assert.throws(function() { Validator.validate(schema, {recordBField2: {}}); });
    assert.throws(function() { Validator.validate(schema, {recordBField1: {'RecordB': {}}, recordBField2: {}}); });
  });
  it('repeated named types', function () {
    var schema = {
      type: 'record',
      name: 'Outer',
      fields: [
        {name: 'first', type: {type: 'record', name: 'Inner', fields: []}},
        {name: 'second', type: {type: 'record', name: 'Inner', fields: []}}
      ]
    };
    assert(Validator.validate(schema, {first: {}, second: {}}));

    schema.fields[1].type.fields.push({name: 'value', type: 'string'});
    assert.throws(function() {
      Validator.validate(schema, {first: {}, second: {value: 'a'}});
    });
  });
  it('deeply nested repeated named types', function () {
    var nestedType = {
      type: 'record',
      name: 'Nested',
      fields: [{
        name: 'leaf',
        type: {
          type: 'record',
          name: 'Leaf',
          fields: [{name: 'value', type: 'string'}]
        }
      }]
    };
    var schema = {
      type: 'record',
      name: 'Outer',
      fields: [
        {name: 'first', type: nestedType},
        {name: 'second', type: JSON.parse(JSON.stringify(nestedType))}
      ]
    };
    var obj = {
      first: {leaf: {value: 'a'}},
      second: {leaf: {value: 'b'}}
    };
    assert(Validator.validate(schema, obj));

    schema.fields[1].type.fields[0].type.fields[0].type = 'int';
    assert.throws(function() {
      Validator.validate(schema, obj);
    });
  });
  it('repeated named enums', function () {
    var enumType = {
      type: 'enum',
      name: 'Choice',
      symbols: ['A', 'B']
    };
    var schema = {
      type: 'record',
      name: 'Outer',
      fields: [
        {name: 'first', type: enumType},
        {name: 'second', type: JSON.parse(JSON.stringify(enumType))}
      ]
    };
    assert(Validator.validate(schema, {first: 'A', second: 'B'}));

    schema.fields[1].type.symbols.push('C');
    assert.throws(function() {
      Validator.validate(schema, {first: 'A', second: 'C'});
    });
  });
  it('nested-record-with-namespace-absolute', function () {
    var schema = {type: 'record', namespace: 'x.y.z', name: 'RecordA', fields: [{name: 'recordBField1', type: ['null', {type: 'record', name: 'RecordB', fields: []}]}, {name: 'recordBField2', type: 'x.y.z.RecordB'}]};
    assert(Validator.validate(schema, {recordBField1: null, recordBField2: {}}));
    assert(Validator.validate(schema, {recordBField1: {'x.y.z.RecordB': {}}, recordBField2: {}}));
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, {recordBField1: null}); });
    assert.throws(function() { Validator.validate(schema, {recordBField2: {}}); });
    assert.throws(function() { Validator.validate(schema, {recordBField1: {'RecordB': {}}, recordBField2: {}}); });
  });


  // Enums
  it('enum', function () {
    var schema = {type: 'enum', name: 'Colors', symbols: ['Red', 'Blue']};
    assert(Validator.validate(schema, 'Red'));
    assert(Validator.validate(schema, 'Blue'));
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, undefined); });
    assert.throws(function() { Validator.validate(schema, 'NotAColor'); });
    assert.throws(function() { Validator.validate(schema, ''); });
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, []); });
    assert.throws(function() { Validator.validate(schema, 1); });
  });

  // Unions
  it('union', function () {
    var schema = ['string', 'int'];
    assert(Validator.validate(schema, {string: 'a'}));
    assert(Validator.validate(schema, {int: 1}));
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, undefined); });
    assert.throws(function() { Validator.validate(schema, 'a'); });
    assert.throws(function() { Validator.validate(schema, 1); });
    assert.throws(function() { Validator.validate(schema, {string: 'a', int: 1}); });
    assert.throws(function() { Validator.validate(schema, []); });
  });

  it('union with null', function () {
    var schema = ['string', 'null'];
    assert(Validator.validate(schema, {string: 'a'}));
    assert(Validator.validate(schema, null));
    assert.throws(function() { Validator.validate(schema, undefined); });
  });

  it('nested union', function () {
    var schema = ['string', {type: 'int'}];
    assert(Validator.validate(schema, {string: 'a'}));
    assert(Validator.validate(schema, {int: 1}));
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, undefined); });
    assert.throws(function() { Validator.validate(schema, 'a'); });
    assert.throws(function() { Validator.validate(schema, 1); });
    assert.throws(function() { Validator.validate(schema, {string: 'a', int: 1}); });
    assert.throws(function() { Validator.validate(schema, []); });
  });

  // Arrays
  it('array', function () {
    var schema = {type: "array", items: "string"};
    assert(Validator.validate(schema, []));
    assert(Validator.validate(schema, ["a"]));
    assert(Validator.validate(schema, ["a", "b", "a"]));
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, undefined); });
    assert.throws(function() { Validator.validate(schema, 'a'); });
    assert.throws(function() { Validator.validate(schema, 1); });
    assert.throws(function() { Validator.validate(schema, {}); });
    assert.throws(function() { Validator.validate(schema, {"1": "a"}); });
    assert.throws(function() { Validator.validate(schema, {1: "a"}); });
    assert.throws(function() { Validator.validate(schema, {1: "a", "b": undefined}); });
    assert.throws(function() { var a = {}; a[0] = "a"; Validator.validate(schema, a); });
    assert.throws(function() { Validator.validate(schema, [1]); });
    assert.throws(function() { Validator.validate(schema, [1, "a"]); });
    assert.throws(function() { Validator.validate(schema, ["a", 1]); });
    assert.throws(function() { Validator.validate(schema, [null, 1]); });
  });

  // Maps
  it('map', function () {
    var schema = {type: "map", values: "string"};
    assert(Validator.validate(schema, {}));
    assert(Validator.validate(schema, function () {}));
    assert(Validator.validate(schema, {"a": "b"}));
    assert(Validator.validate(schema, {"a": "b", "c": "d"}));
    assert.throws(function() { Validator.validate(schema, null); });
    assert.throws(function() { Validator.validate(schema, undefined); });
    assert.throws(function() { Validator.validate(schema, 'a'); });
    assert.throws(function() { Validator.validate(schema, 1); });
    assert.throws(function() { Validator.validate(schema, [1]); });
    assert.throws(function() { Validator.validate(schema, {"a": 1}); });
    assert.throws(function() { Validator.validate(schema, {"a": "b", "c": 1}); });
  });

  // Protocols
  it('protocol', function () {
    var protocol = {protocol: "Protocol1", namespace: "x.y.z", types: [
      {type: "record", name: "RecordA", fields: []},
      {type: "record", name: "RecordB", fields: [{name: "recordAField", type: "RecordA"}]}
    ]};
    assert(ProtocolValidator.validate(protocol, 'RecordA', {}));
    assert(ProtocolValidator.validate(protocol, 'x.y.z.RecordA', {}));
    assert(ProtocolValidator.validate(protocol, 'RecordB', {recordAField: {}}));
    assert(ProtocolValidator.validate(protocol, 'x.y.z.RecordB', {recordAField: {}}));
    assert.throws(function() { ProtocolValidator.validate(protocol, 'RecordDoesNotExist', {}); });
    assert.throws(function() { ProtocolValidator.validate(protocol, 'RecordDoesNotExist', null); });
    assert.throws(function() { ProtocolValidator.validate(protocol, 'RecordB', {}); });
    assert.throws(function() { ProtocolValidator.validate(protocol, null, {}); });
    assert.throws(function() { ProtocolValidator.validate(protocol, '', {}); });
    assert.throws(function() { ProtocolValidator.validate(protocol, {}, {}); });
  });
  it('invalid protocol object', function () {
    assert.throws(function() { return new ProtocolValidator(); });
    assert.throws(function() { return new ProtocolValidator(null); });
  });

  // Samples
  it('link', function () {
    var schema = {
      "type" : "record",
      "name" : "Bundle",
      "namespace" : "aa.bb.cc",
      "fields" : [ {
        "name" : "id",
        "type" : "string"
      }, {
        "name" : "type",
        "type" : "string"
      }, {
        "name" : "data_",
        "type" : [ "null", {
          "type" : "record",
          "name" : "LinkData",
          "fields" : [ {
            "name" : "address",
            "type" : "string"
          }, {
            "name" : "title",
            "type" : [ "null", "string" ],
            "default" : null
          }, {
            "name" : "excerpt",
            "type" : [ "null", "string" ],
            "default" : null
          }, {
            "name" : "image",
            "type" : [ "null", {
              "type" : "record",
              "name" : "Image",
              "fields" : [ {
                "name" : "url",
                "type" : "string"
              }, {
                "name" : "width",
                "type" : "int"
              }, {
                "name" : "height",
                "type" : "int"
              } ]
            } ],
            "default" : null
          }, {
            "name" : "meta",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          } ]
        } ],
        "default" : null
      }, {
        "name" : "atoms_",
        "type" : {
          "type" : "map",
          "values" : {
            "type" : "map",
            "values" : {
              "type" : "record",
              "name" : "Atom",
              "fields" : [ {
                "name" : "index_",
                "type" : {
                  "type" : "record",
                  "name" : "AtomIndex",
                  "fields" : [ {
                    "name" : "type_",
                    "type" : "string"
                  }, {
                    "name" : "id",
                    "type" : "string"
                  } ]
                }
              }, {
                "name" : "data_",
                "type" : [ "LinkData" ]
              } ]
            }
          }
        },
        "default" : {
        }
      }, {
        "name" : "meta_",
        "type" : {
          "type" : "record",
          "name" : "BundleMetadata",
          "fields" : [ {
            "name" : "date",
            "type" : "long",
            "default" : 0
          }, {
            "name" : "members",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "tags",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "meta",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "votes",
            "type" : {
              "type" : "map",
              "values" : {
                "type" : "record",
                "name" : "VoteData",
                "fields" : [ {
                  "name" : "date",
                  "type" : "long"
                }, {
                  "name" : "userName",
                  "type" : [ "null", "string" ],
                  "default" : null
                }, {
                  "name" : "direction",
                  "type" : {
                    "type" : "enum",
                    "name" : "VoteDirection",
                    "symbols" : [ "Up", "Down", "None" ]
                  }
                } ]
              }
            },
            "default" : {
            }
          }, {
            "name" : "views",
            "type" : {
              "type" : "map",
              "values" : {
                "type" : "record",
                "name" : "ViewData",
                "fields" : [ {
                  "name" : "userName",
                  "type" : "string"
                }, {
                  "name" : "count",
                  "type" : "int"
                } ]
              }
            },
            "default" : {
            }
          }, {
            "name" : "relevance",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          }, {
            "name" : "clicks",
            "type" : {
              "type" : "map",
              "values" : "string"
            },
            "default" : {
            }
          } ]
        }
      } ]
    };
    var okObj = {
      "id": "https://github.com/sqs/akka-kryo-serialization/subscription",
      "type": "link",
      "data_": {
        "aa.bb.cc.LinkData": {
          "address": "https://github.com/sqs/akka-kryo-serialization/subscription",
          "title": {
            "string": "Sign in · GitHub"
          },
          "excerpt": {
            "string": "Signup and Pricing Explore GitHub Features Blog Sign in Sign in (Pricing and Signup) Username or Email Password (forgot password) GitHub Links GitHub About Blog Feat"
          },
          "image": {
            "aa.bb.cc.Image": {
              "url": "https://a248.e.akamai.net/assets.github.com/images/modules/header/logov7@4x.png?1340659561",
              "width": 280,
              "height": 120
            }
          },
          "meta": {}
        }
      },
      "atoms_": {
        "link": {
          "https://github.com/sqs/akka-kryo-serialization/subscription": {
            "index_": {
              "type_": "link",
              "id": "https://github.com/sqs/akka-kryo-serialization/subscription"
            },
            "data_": {
              "aa.bb.cc.LinkData": {
                "address": "https://github.com/sqs/akka-kryo-serialization/subscription",
                "title": {
                  "string": "Sign in · GitHub"
                },
                "excerpt": {
                  "string": "Signup and Pricing Explore GitHub Features Blog Sign in Sign in (Pricing and Signup) Username or Email Password (forgot password) GitHub Links GitHub About Blog Feat"
                },
                "image": {
                  "aa.bb.cc.Image": {
                    "url": "https://a248.e.akamai.net/assets.github.com/images/modules/header/logov7@4x.png?1340659561",
                    "width": 280,
                    "height": 120
                  }
                },
                "meta": {}
              }
            }
          }
        }
      },
      "meta_": {
        "date": 1345537530000,
        "members": {
          "a@a.com": "1"
        },
        "tags": {
          "blue": "1"
        },
        "meta": {},
        "votes": {},
        "views": {
          "a@a.com": {
            "userName": "John Smith",
            "count": 100
          }
        },
        "relevance": {
          "a@a.com": "1",
          "b@b.com": "2"
        },
        "clicks": {}
      }
    };

    assert(Validator.validate(schema, okObj));

    var badObj = okObj; // no deep copy since we won't reuse okObj
    badObj.meta_.clicks['a@a.com'] = 123;
    assert.throws(function() { Validator.validate(schema, badObj); });

  });

});
