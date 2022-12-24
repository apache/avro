use apache_avro::{to_avro_datum, to_value, Schema};

#[test]
fn test_multivalue_union_tovalue() {
    let nuschema = r#"
    {
        "type": "record",
        "namespace": "datatypes",
        "name": "nullunion",
        "fields":
        [
            {"name": "item",
                "type": [
                    "null",
                    "long",
                    "double"
                ]
            }
        ]
    }
    "#;

    let nullunion = NullUnion {
        item: Some(UnionLongDouble::Long(34)),
    };

    let schema = Schema::parse_str(nuschema).unwrap();

    let nu_value = to_value(nullunion).unwrap();
    let nu_encoded = to_avro_datum(&schema, nu_value).unwrap();

    println!("{:?}", nu_encoded);
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]

pub enum UnionLongDouble {
    Long(i64),
    Double(f64),
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct NullUnion {
    pub item: Option<UnionLongDouble>,
}

impl From<i64> for UnionLongDouble {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

// impl TryFrom<UnionLongDouble> for i64 {
//     type Error = UnionLongDouble;

//     fn try_from(v: UnionLongDouble) -> Result<Self, Self::Error> {
//         if let UnionLongDouble::Long(v) = v {
//             Ok(v)
//         } else {
//             Err(v)
//         }
//     }
// }

impl From<f64> for UnionLongDouble {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

// impl TryFrom<UnionLongDouble> for f64 {
//     type Error = UnionLongDouble;

//     fn try_from(v: UnionLongDouble) -> Result<Self, Self::Error> {
//         if let UnionLongDouble::Double(v) = v {
//             Ok(v)
//         } else {
//             Err(v)
//         }
//     }
// }
