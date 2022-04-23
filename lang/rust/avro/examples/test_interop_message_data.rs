use apache_avro::{schema::AvroSchema, types::Value};



struct InteropMessage;


impl AvroSchema for InteropMessage {
    fn get_schema() -> apache_avro::Schema {    
        let schema = std::fs::read_to_string("../../share/test/data/messageV1/test_schema.json").expect("File should exist with schema inside");
        apache_avro::Schema::parse_str(schema.as_str()).expect("File should exist with schema inside")
    }
}

impl Into<Value> for InteropMessage {
    fn into(self) -> Value {
        Value::Record(vec![
            ("id".into(), 42i64.into()),
            ("name".into(), "Bill".into()),
            ("tags".into(), Value::Array(vec!["dog_lover", "cat_hater"].into_iter().map(|s| s.into()).collect()))
        ])
    }
}

fn main() {
    let file_message = std::fs::read("../../share/test/data/messageV1/test_message.bin").expect("File not found or error reading");
    let mut generated_encoding: Vec<u8> = Vec::new();
    apache_avro::SingleObjectWriter::<InteropMessage>::with_capacity(1024).expect("resolve expected").write_value(InteropMessage, &mut generated_encoding).expect("Should encode");
    assert_eq!(file_message, generated_encoding)
}
