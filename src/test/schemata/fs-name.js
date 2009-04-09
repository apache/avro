
{"namespace": "org.apache.hadoop.fs",
 "protocol": "FileSystem",

 "types": [
     {"name": "FileStatus", "type": "record",
      "fields": {
          "path": "string",
          "size": "long"
      }
     },
      
     {"name": "BlockLocation", "type": "record",
      "fields": {
          "hosts": {"type": "array", "items": "string"},
          "offset": "long",
          "length": "long"
      }
     },

     {"name": "NotPermitted", "type": "error", "fields": {}},

     {"name": "FileNotFound", "type": "error", "fields": { "path": "string" }}

 ],

 "messages": {

     "getStatus": {
         "request": {"path": "string"},
         "response": "FileStatus",
         "errors": ["NotPermitted", "FileNotFound"]
     },

     "listStatus": {
         "request": {"path": "string"},
         "response": {"type": "array", "items": "FileStatus"},
         "errors": ["FileNotFound", "NotPermitted"]
     },

     "getLocations": {
         "request": {"path": "string", "start": "long", "length": "long"},
         "response": {"type": "array", "items": "BlockLocation"},
         "errors": ["FileNotFound", "NotPermitted"]
     }

 }

}
