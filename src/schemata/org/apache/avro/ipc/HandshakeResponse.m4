{
    "type": "record",
    "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
    "fields": [
        {"name": "match",
         "type": {"type": "enum", "name": "HandshakeMatch",
                  "symbols": ["BOTH", "CLIENT", "NONE"]}},
        {"name": "serverProtocol",
         "type": ["null", "string"]},
        {"name": "serverHash",
         "type": ["null", include(`org/apache/avro/ipc/MD5.js')]},
 	{"name": "meta",
         "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
}
