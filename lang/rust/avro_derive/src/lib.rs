// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use proc_macro2::{Span, TokenStream, TokenTree};
use quote::quote;

use syn::{parse_macro_input, Attribute, DeriveInput, Error, Lit, Path, Type, TypePath};

#[proc_macro_derive(AvroSchema, attributes(namespace))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(&mut input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: &mut DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let namespace = get_namespace_from_attributes(&input.attrs)?;
    let full_schema_name = vec![namespace, Some(input.ident.to_string())]
        .into_iter()
        .flatten()
        .collect::<Vec<String>>()
        .join(".");
    let schema_def = match &input.data {
        syn::Data::Struct(s) => {
            get_data_struct_schema_def(&full_schema_name, s, input.ident.span())?
        }
        syn::Data::Enum(e) => get_data_enum_schema_def(&full_schema_name, e, input.ident.span())?,
        _ => {
            return Err(vec![Error::new(
                input.ident.span(),
                "AvroSchema derive only works for structs and simple enums ",
            )])
        }
    };

    let ty = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics apache_avro::schema::AvroSchemaComponent for #ty #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut HashMap<apache_avro::schema::Name, apache_avro::schema::Schema>, enclosing_namespace: &Option<String>) -> apache_avro::schema::Schema {
                let name =  apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse schema name {}", #full_schema_name)[..]).fully_qualified_name(enclosing_namespace);
                let enclosing_namespace = &name.namespace;
                if named_schemas.contains_key(&name) {
                    apache_avro::schema::Schema::Ref{name: name.clone()}
                } else {
                    named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
                    #schema_def
                }
            }
        }
    })
}

fn get_namespace_from_attributes(attrs: &[Attribute]) -> Result<Option<String>, Vec<Error>> {
    let namespace_attr_path_constant: Path = syn::parse2::<Path>(quote! {namespace}).unwrap();
    const NAMESPACE_PARSING_ERROR_CONSTANST: &str =
        "Namespace attribute must be in form #[namespace = \"com.testing.namespace\"]";
    // parse out namespace if present. Requires strict syntax
    for attr in attrs {
        if namespace_attr_path_constant == attr.path {
            let mut input_tokens = attr.tokens.clone().into_iter();
            if let (
                Some(TokenTree::Punct(punct)),
                Some(TokenTree::Literal(namespace_literal)),
                None,
            ) = (
                input_tokens.next(),
                input_tokens.next(),
                input_tokens.next(),
            ) {
                if punct.as_char() == '=' {
                    if let Lit::Str(lit_str) = Lit::new(namespace_literal) {
                        return Ok(Some(lit_str.value()));
                    }
                }
            }
            return Err(vec![Error::new_spanned(
                &attr.tokens,
                NAMESPACE_PARSING_ERROR_CONSTANST,
            )]);
        }
    }
    Ok(None)
}

fn get_data_struct_schema_def(
    full_schema_name: &str,
    s: &syn::DataStruct,
    error_span: Span,
) -> Result<TokenStream, Vec<Error>> {
    let mut record_field_exprs = vec![];
    match s.fields {
        syn::Fields::Named(ref a) => {
            for (position, field) in a.named.iter().enumerate() {
                let name = field.ident.as_ref().unwrap().to_string(); // we know everything has a name
                let schema_expr = type_to_schema_expr(&field.ty)?;
                let position = position;
                record_field_exprs.push(quote! {
                    apache_avro::schema::RecordField {
                            name: #name.to_string(),
                            doc: Option::None,
                            default: Option::None,
                            schema: #schema_expr,
                            order: apache_avro::schema::RecordFieldOrder::Ignore,
                            position: #position,
                        }
                });
            }
        }
        syn::Fields::Unnamed(_) => {
            return Err(vec![Error::new(
                error_span,
                "AvroSchema derive does not work for tuple structs",
            )])
        }
        syn::Fields::Unit => {
            return Err(vec![Error::new(
                error_span,
                "AvroSchema derive does not work for unit structs",
            )])
        }
    }
    Ok(quote! {
        let schema_fields = vec![#(#record_field_exprs),*];
        let name = apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse struct name for schema {}", #full_schema_name)[..]);
        apache_avro::schema::record_schema_for_fields(name, None, None, schema_fields)
    })
}

fn get_data_enum_schema_def(
    full_schema_name: &str,
    e: &syn::DataEnum,
    error_span: Span,
) -> Result<TokenStream, Vec<Error>> {
    if e.variants.iter().all(|v| syn::Fields::Unit == v.fields) {
        let symbols: Vec<String> = e
            .variants
            .iter()
            .map(|varient| varient.ident.to_string())
            .collect();
        Ok(quote! {
            apache_avro::schema::Schema::Enum {
                name: apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse enum name for schema {}", #full_schema_name)[..]),
                aliases: None,
                doc: None,
                symbols: vec![#(#symbols.to_owned()),*]
            }
        })
    } else {
        Err(vec![Error::new(
            error_span,
            "AvroSchema derive does not work for enums with non unit structs",
        )])
    }
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<Error>> {
    if let Type::Path(p) = ty {
        let type_string = p.path.segments.last().unwrap().ident.to_string();

        let schema = match &type_string[..] {
            "bool" => quote! {Schema::Boolean},
            "i8" | "i16" | "i32" | "u8" | "u16" => quote! {apache_avro::schema::Schema::Int},
            "i64" => quote! {apache_avro::schema::Schema::Long},
            "f32" => quote! {apache_avro::schema::Schema::Float},
            "f64" => quote! {apache_avro::schema::Schema::Double},
            "String" | "str" => quote! {apache_avro::schema::Schema::String},
            "char" => {
                return Err(vec![Error::new_spanned(
                    ty,
                    "AvroSchema: Cannot guarentee sucessful deserialization of this type",
                )])
            }
            "u32" | "u64" => {
                return Err(vec![Error::new_spanned(
                ty,
                "Cannot guarentee sucessful serialization of this type due to overflow concerns",
            )])
            } //Can't guarentee serialization type
            _ => {
                // Fails when the type does not implement AvroSchemaComponent directly
                // TODO check and error report with something like https://docs.rs/quote/1.0.15/quote/macro.quote_spanned.html#example
                type_path_schema_expr(p)
            }
        };
        Ok(schema)
    } else if let Type::Array(ta) = ty {
        let inner_schema_expr = type_to_schema_expr(&ta.elem)?;
        Ok(quote! {apache_avro::schema::Schema::Array(Box::new(#inner_schema_expr))})
    } else if let Type::Reference(tr) = ty {
        type_to_schema_expr(&tr.elem)
    } else {
        Err(vec![Error::new_spanned(
            ty,
            format!("Unable to generate schema for type: {:?}", ty),
        )])
    }
}

/// Generates the schema def expression for fully qualified type paths using the associated function
/// - `A -> <A as AvroSchemaComponent>::get_schema_in_ctxt()`
/// - `A<T> -> <A<T> as AvroSchemaComponent>::get_schema_in_ctxt()`
fn type_path_schema_expr(p: &TypePath) -> TokenStream {
    quote! {<#p as apache_avro::schema::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn basic_case() {
        let test_struct = quote! {
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn tuple_struct_unsupported() {
        let test_tuple_struct = quote! {
            struct B (i32, String);
        };

        match syn::parse2::<DeriveInput>(test_tuple_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn unit_struct_unsupported() {
        let test_tuple_struct = quote! {
            struct AbsoluteUnit;
        };

        match syn::parse2::<DeriveInput>(test_tuple_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn struct_with_optional() {
        let stuct_with_optional = quote! {
            struct Test4 {
                a : Option<i32>
            }
        };
        match syn::parse2::<DeriveInput>(stuct_with_optional) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_basic_enum() {
        let basic_enum = quote! {
            enum Basic {
                A,
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(basic_enum) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_namespace() {
        let test_struct = quote! {
            #[namespace = "namespace.testing"]
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok());
                assert!(derive_avro_schema(&mut input)
                    .unwrap()
                    .to_string()
                    .contains("namespace.testing"))
            }
            Err(error) => panic!(
                "Failied to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_reference() {
        let test_reference_struct = quote! {
            struct A<'a> {
                a: &'a Vec<i32>,
                b: &'static str
            }
        };

        match syn::parse2::<DeriveInput>(test_reference_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }
}
