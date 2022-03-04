use proc_macro2::TokenStream;
use quote::quote;

use syn::{parse_macro_input, DeriveInput, Error, PathArguments, Type, TypePath};

#[proc_macro_derive(AvroSchema)]
/// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(&mut input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: &mut DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let schema_def = match &input.data {
        syn::Data::Struct(s) => get_data_struct_schema_def(s, &input.ident)?,
        syn::Data::Enum(e) => get_data_enum_schema_def(e, &input.ident)?,
        _ => {
            return Err(vec![Error::new(
                input.ident.span(),
                "AvroSchema derive only works for structs",
            )])
        }
    };

    let ty = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics AvroSchema for #ty #ty_generics #where_clause {
            fn get_schema() -> Schema {
                #schema_def
            }
        }
    })
}

fn get_data_struct_schema_def(
    s: &syn::DataStruct,
    ident: &syn::Ident,
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
                ident.span(),
                "AvroSchema derive does not work for tuple structs",
            )])
        }
        syn::Fields::Unit => {
            return Err(vec![Error::new(
                ident.span(),
                "AvroSchema derive does not work for unit structs",
            )])
        }
    }
    let name = ident.to_string();
    Ok(quote! {
        let schema_fields = vec![#(#record_field_exprs),*];
        apache_avro::schema::record_schema_for_fields(apache_avro::schema::Name::new(#name), None, schema_fields)
    })
}

fn get_data_enum_schema_def(
    e: &syn::DataEnum,
    ident: &syn::Ident,
) -> Result<TokenStream, Vec<Error>> {
    if e.variants.iter().all(|v| syn::Fields::Unit == v.fields) {
        let symbols: Vec<String> = e
            .variants
            .iter()
            .map(|varient| varient.ident.to_string())
            .collect();
        let name = ident.to_string();
        Ok(quote! {
            apache_avro::schema::Schema::Enum {
                name: apache_avro::schema::Name::new(#name),
                doc: None,
                symbols: vec![#(#symbols.to_owned()),*]
            }
        })
    } else {
        Err(vec![Error::new(
            ident.span(),
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
            "String" => quote! {apache_avro::schema::Schema::String},
            "char" => {
                return Err(vec![Error::new_spanned(
                    ty,
                    "AvroSchema: Cannot guarentee sucessful deserialization of this type",
                )])
            }
            "u32" | "u64" => return Err(vec![Error::new_spanned(
                ty,
                "Cannot guarentee sucessful serialization of this type due to overflow concerns",
            )]), //Can't guarentee serialization type
            _ => {
                // Fails when the type does not implement AvroSchema directly or covered by blanket implementation
                // TODO check and error report with something like https://docs.rs/quote/1.0.15/quote/macro.quote_spanned.html#example
                return type_path_get_schema(p);
            }
        };
        Ok(schema)
    } else if let Type::Array(ta) = ty {
        let inner_schema_expr = type_to_schema_expr(&ta.elem)?;
        Ok(quote! {apache_avro::schema::Schema::Array(Box::new(#inner_schema_expr))})
    } else {
        Err(vec![])
    }
}

/// Generates the schema def expression for fully qualified type paths using the associated function
/// - `A -> A::get_schema()`
/// - `A<T> -> A::<T>::get_schema()`
/// - `crate::mod::mod::A<T> -> crate::mod::mod::A::<T>:get_schema()`
/// TODO review if need to implement as
/// - `A -> <A as AvroSchema>::get_schema()`
/// - `A<T> -> <A<T> as AvroSchema>::get_schema()`
///
fn type_path_get_schema(p: &TypePath) -> Result<TokenStream, Vec<Error>> {
    let last = p.path.segments.last().unwrap(); // inside a stuct the type must always be defined / have a last path segment
    let mut it = p.path.segments.iter().peekable();
    let mut all_but_last = vec![];
    while let Some(path_seg) = it.next() {
        if let Some(_) = it.peek() {
            //not the last
            all_but_last.push(path_seg.clone());
        }
    }

    let ident = last.ident.clone();
    match last.arguments.clone() {
        PathArguments::None => Ok(quote! {#(#all_but_last::)*#ident::get_schema()}),
        PathArguments::AngleBracketed(a) => {
            Ok(quote! {#(#all_but_last::)*#ident::#a::get_schema()})
        }
        PathArguments::Parenthesized(_) => unreachable!(),
    }
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
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
                println!("{}", derive_avro_schema(&mut input).unwrap());
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(_) => assert!(false),
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
            Err(_) => assert!(false),
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
            Err(_) => assert!(false),
        };
    }

    #[test]
    fn optional_type_generating() {
        let stuct_with_optional = quote! {
            struct Test4 {
                a : Option<i32>
            }
        };
        match syn::parse2::<DeriveInput>(stuct_with_optional) {
            Ok(mut input) => {
                println!("{}", derive_avro_schema(&mut input).unwrap());
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(_) => assert!(false),
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
                println!("{}", derive_avro_schema(&mut input).unwrap());
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(_) => assert!(false),
        };
    }
}
