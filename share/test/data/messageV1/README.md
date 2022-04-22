BinaryMessage data in single object encoding https://avro.apache.org/docs/current/spec.html#single_object_encoding

Ground truth data generated with Java Code

The binary data will be the V1 sincle object encoding with the schema of
```
{
	"type":"record",
	"namespace":"org.apache.avro",
	"name":"TestMessage",
	"fields":[
		{
			"name":"id",
			"type":"long"
		},
		{
			"name":"name",
			"type":"string"
		},
		{
			"name":"tags",
			"type":{
				"type":"array",
				"items":"string"
			}
		},
		{
			"name":"scores",
			"type":{
				"type":"map",
				"values":"double"
			}
		}
	]
}
```