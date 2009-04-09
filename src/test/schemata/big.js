{"type": "object",
 "name": "big",
 "properties": {
     "stringField": {"type": "string"},
     "bytesField": {"type": "bytes"},
     "longField": {"type": "long"},
     "doubleField": {"type": "double"},
     "booleanField": {"type": "boolean"},
     "arrayField": {"type": "array", "items": {"type": "long"}},
     "objArray": {"type": "array",
		  "items": {"type": "object",
			    "properties": {"foo": {"type": "long"}}}},
     "objectField": {"type": "object",
		     "properties": {
			 "nestedLong": {"type": "long"},
			 "nestedString": {"type": "string"}}}}}
