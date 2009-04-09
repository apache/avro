{"type": "record", "name":"Interop", "namespace": "org.apache.avro",
  "fields": {
      "intField": "int",
      "longField": "long",
      "stringField":"string",
      "boolField":"boolean",
      "floatField":"float",
      "doubleField":"double",
      "bytesField":"bytes",
      "nullField":"null",
      "arrayField":{"type":"array", "items": "double"},
      "mapField":{"type":"map", "keys": "long", "values": 
        {"type": "record", "name": "Foo", "fields":{"label": "string"}}},
      "unionField": ["boolean", "double", {"type":"array","items": "bytes"}],
      "recordField":{"type": "record", "name": "Node",
        "fields":{"label": "string","children": {"type":
        "array", "items": "Node" }}}
      }
}
