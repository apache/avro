
{"namespace": "org.apache.hadoop.fs",

 "types": {
     "Node": {
         "type": "object",
         "properties": {
             "name": {"type": "string"},
             "children": {"type": "array", "items": "Node"}
         }
     }
 }
}
