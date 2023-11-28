use apache_avro::{
    schema_equality::{set_schemata_equality_comparator, SchemataEq, StructFieldEq},
    types::Record,
    Codec, Schema, Writer,
};
use apache_avro_test_helper::TestResult;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;

#[test]
#[ignore]
fn test_11111() -> TestResult {
    let schema_str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "field_name",
          "type": "bytes",
          "logicalType": "decimal", "precision": 4,    "scale": 2
        }
      ]
    }
    "#;
    let schema = Schema::parse_str(schema_str)?;

    let mut record = Record::new(&schema).unwrap();
    let val = BigDecimal::new(BigInt::from(12), 2);
    record.put("field_name", val);

    let codec = Codec::Null;
    let mut writer = Writer::builder()
        .schema(&schema)
        .codec(codec)
        .writer(Vec::new())
        .build();

    writer.append(record.clone())?;

    Ok(())
}

#[test]
fn test_avro_3939_22222() -> Result<(), Box<dyn SchemataEq>> {
    let a = StructFieldEq {
        include_attributes: false,
    };

    set_schemata_equality_comparator(Box::new(a))?;

    Ok(())
}
