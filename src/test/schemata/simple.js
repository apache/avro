{"namespace": "org.apache.avro.test",
 "protocol": "Simple",

 "types": [
     {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]},

     {"name": "TestRecord", "type": "record",
      "fields": [
          {"name": "name", "type": "string"},
          {"name": "kind", "type": "Kind"}
      ]
     },

     {"name": "TestError", "type": "error", "fields": [
         {"name": "message", "type": "string"}
      ]
     }

 ],

 "messages": {

     "hello": {
         "request": [{"name": "greeting", "type": "string"}],
         "response": "string"
     },

     "echo": {
         "request": [{"name": "record", "type": "TestRecord"}],
         "response": "TestRecord"
     },

     "echoBytes": {
         "request": [{"name": "data", "type": "bytes"}],
         "response": "bytes"
     },

     "error": {
         "request": [],
         "response": "null",
         "errors": ["TestError"]
     }
 }

}
