BinaryMessage data in single object encoding https://avro.apache.org/docs/current/spec.html#single_object_encoding

Ground truth data generated with Java Code

The binary data will be the V1 single object encoding with the schema of
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

The sample binary message will have the values equal to the json serialized version of the record shown below
```
{
	"id": 42,
	"name": "Bill",
	"tags": ["dog_lover", "cat_hater"]
}
```
