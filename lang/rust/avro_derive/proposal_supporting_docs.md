The trait defined within schema.rs
```
pub trait AvroSchema {
    fn get_schema() -> Schema;
}
```

##### Reasoning/Desires 
Associated funtion as the implementation. Not associated const to make schema creation function easier (can use non const functions). The best would be to have this associated function return &'static Schema but I have yet to figure out how to do that without some global state which is undesirable. 

##### Desired user workflow 
Anything that can be serialized the "The serde way" should be able to be serialized/deserialized without further configuration. 

Current Flow
```
use apache_avro::Schema;

let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }

use apache_avro::Writer;

#[derive(Debug, Serialize)]
struct Test {
    a: i64,
    b: String,
}

// if the schema is not valid, this function will return an error
let schema = Schema::parse_str(raw_schema).unwrap();

let mut writer = Writer::new(&schema, Vec::new());
let test = Test {
    a: 27,
    b: "foo".to_owned(),
};
writer.append_ser(test).unwrap();
let encoded = writer.into_inner();
```

New Flow
```
use apache_avro::Writer;

#[derive(Debug, Serialize, AvroSchema)]
struct Test {
    a: i64,
    b: String,
}
// derived schema, always valid or code fails to compile with a descriptive message
let schema = Test::get_schema();

let mut writer = Writer::new(&schema, Vec::new());
let test = Test {
    a: 27,
    b: "foo".to_owned(),
};
writer.append_ser(test).unwrap();
let encoded = writer.into_inner();
```


##### crate import
To use this functionality it comes as an optional feature (modeled off serde)

cargo.toml
```
apache-avro = { version = "X.Y.Z", features = ["derive"] }
```