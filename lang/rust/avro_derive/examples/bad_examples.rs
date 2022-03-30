use apache_avro::schema::{AvroSchema, AvroSchemaWithResolved};
use apache_avro::{from_value, Reader, Schema, Writer};
use avro_derive::*;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::collections::HashMap;

#[macro_use]
extern crate serde;

/// This module should not compile. This is an examples page for many common errors when using the avro_derive functionality. The errors should be handled gracefully, and explained in detail here.
///
mod examples {}
fn main() {}
