{"namespace": "org.apache.avro",
 "protocol": "Test",

 "types": [
     {"name": "TestRecord", "type": "record",
      "fields": {
          "name": "string"
      }
     },
      
     {"name": "TestError", "type": "error", "fields": {"message": "string"}}

 ],

 "messages": {

     "hello": {
         "request": {"greeting": "string" },
         "response": "string"
     },

     "echo": {
         "request": {"record": "TestRecord" },
         "response": "TestRecord"
     },

     "echoBytes": {
         "request": {"data": "bytes" },
         "response": "bytes"
     },

     "error": {
         "request": {},
         "response": "null",
         "errors": ["TestError"]
     }
 }

}
