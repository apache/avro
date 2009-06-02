{
    "type": "record",
    "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
    "fields": [
        {"name": "clientHash",
	 "type": include(`org/apache/avro/ipc/MD5.js')},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
 	{"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
 ]
}
