use apache_avro::schema::{AvroSchema, AvroSchemaWithResolved};
use apache_avro::{from_value, Reader, Schema, Writer};
use avro_derive::*;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::collections::HashMap;

#[macro_use]
extern crate serde;

fn main() {}
