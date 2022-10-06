use apache_avro::to_value;
use apache_avro::types::Value;
use serde::{Deserialize, Serialize};

#[test]
fn avro_3631_visibility_of_avro_serialize_bytes_type() {
    use apache_avro::{avro_serialize_bytes, avro_serialize_fixed};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestStructFixedField<'a> {
        // will be serialized as Value::Bytes
        #[serde(serialize_with = "avro_serialize_bytes")]
        bytes_field: &'a [u8],

        // will be serialized as Value::Fixed
        #[serde(serialize_with = "avro_serialize_fixed")]
        fixed_field: [u8; 6],
    }

    let test = TestStructFixedField {
        bytes_field: &[2, 22, 222],
        fixed_field: [1; 6],
    };

    let expected = Value::Record(vec![
        (
            "bytes_field".to_owned(),
            Value::Bytes(Vec::from(test.bytes_field.clone())),
        ),
        (
            "fixed_field".to_owned(),
            Value::Fixed(6, Vec::from(test.fixed_field.clone())),
        ),
    ]);

    assert_eq!(expected, to_value(test).unwrap());
}
