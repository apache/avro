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

extern crate core;
extern crate darling;

use darling::FromAttributes;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse_macro_input, spanned::Spanned, AttrStyle, Attribute, DeriveInput, Field, GenericArgument,
    PathSegment, Type, TypePath,
};

#[derive(FromAttributes)]
#[darling(attributes(avro))]
struct FieldOptions {
    #[darling(default)]
    doc: Option<String>,
    #[darling(default)]
    default: Option<String>,
    #[darling(default)]
    rename: Option<String>,
    #[darling(default)]
    skip: Option<bool>,
}

#[derive(FromAttributes)]
#[darling(attributes(avro))]
struct NamedTypeOptions {
    #[darling(default)]
    namespace: Option<String>,
    #[darling(default)]
    doc: Option<String>,
    #[darling(multiple)]
    alias: Vec<String>,
}

#[proc_macro_derive(AvroSchema, attributes(avro))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(&mut input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

#[proc_macro_derive(AvroValue)]
pub fn proc_macro_derive_avro_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_avro_value(&input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: &mut DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let named_type_options =
        NamedTypeOptions::from_attributes(&input.attrs[..]).map_err(darling_to_syn)?;
    let full_schema_name = vec![named_type_options.namespace, Some(input.ident.to_string())]
        .into_iter()
        .flatten()
        .collect::<Vec<String>>()
        .join(".");
    let schema_def = match &input.data {
        syn::Data::Struct(s) => get_data_struct_schema_def(
            &full_schema_name,
            named_type_options
                .doc
                .or_else(|| extract_outer_doc(&input.attrs)),
            named_type_options.alias,
            s,
            input.ident.span(),
        )?,
        syn::Data::Enum(e) => get_data_enum_schema_def(
            &full_schema_name,
            named_type_options
                .doc
                .or_else(|| extract_outer_doc(&input.attrs)),
            named_type_options.alias,
            e,
            input.ident.span(),
        )?,
        _ => {
            return Err(vec![syn::Error::new(
                input.ident.span(),
                "AvroSchema derive only works for structs and simple enums ",
            )])
        }
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics apache_avro::schema::derive::AvroSchemaComponent for #ident #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut std::collections::HashMap<apache_avro::schema::Name, apache_avro::schema::Schema>, enclosing_namespace: &Option<String>) -> apache_avro::schema::Schema {
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

fn derive_avro_value(input: &DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    match &input.data {
        syn::Data::Struct(data_struct) => derive_avro_value_struct(input, data_struct),
        syn::Data::Enum(data_enum) => derive_avro_value_enum(input, data_enum),
        _ => Err(vec![syn::Error::new(
            input.ident.span(),
            "AvroValue derive only works for structs and simple enums ",
        )]),
    }
}

fn derive_avro_value_struct(
    input: &DeriveInput,
    data_struct: &syn::DataStruct,
) -> Result<TokenStream, Vec<syn::Error>> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (from_field_assigns, into_fields) = get_data_struct_value_defs(data_struct, ident.span())?;
    Ok(quote! {
        impl #impl_generics From<#ident #ty_generics> for apache_avro::types::Value #where_clause {
            fn from(value: #ident #ty_generics) -> Self {
                apache_avro::types::Value::Record(vec![
                    #(#into_fields)*
                ])
            }
        }
        impl #impl_generics TryFrom<apache_avro::types::Value> for #ident #ty_generics #where_clause {
            type Error = apache_avro::Error;

            fn try_from(value: apache_avro::types::Value) -> Result<Self, Self::Error> {
                let record_fields = match value {
                        apache_avro::types::Value::Record(fields)  => fields,
                        other_value => Err(Self::Error::GetRecord{
                            expected: vec![],
                            other: other_value.into(),
                        })?
                    };
                Ok(Self {
                    #(#from_field_assigns)*
                })
            }
        }
    })
}

fn derive_avro_value_enum(
    input: &DeriveInput,
    data_enum: &syn::DataEnum,
) -> Result<TokenStream, Vec<syn::Error>> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (into_matches, from_matches) =
        get_data_enum_value_defs(ident, data_enum, input.ident.span())?;
    let nsymbols = data_enum.variants.len();
    Ok(quote! {
        impl #impl_generics From<#ident #ty_generics> for apache_avro::types::Value #where_clause {
            fn from(value: #ident #ty_generics) -> Self {
                match value {
                    #(#into_matches),*,
                }
            }
        }
        impl #impl_generics TryFrom<apache_avro::types::Value> for #ident #ty_generics #where_clause {
            type Error = apache_avro::Error;

            fn try_from(value: apache_avro::types::Value) -> Result<Self, Self::Error> {
                match value {
                    #(#from_matches),*,
                    apache_avro::types::Value::Enum(index, _) => {
                        Err(Self::Error::GetEnumValue{index: index as usize, nsymbols: #nsymbols})
                    },
                    other => Err(Self::Error::GetEnum(other.into()))
                }
            }
        }
    })
}

fn get_data_struct_schema_def(
    full_schema_name: &str,
    record_doc: Option<String>,
    aliases: Vec<String>,
    s: &syn::DataStruct,
    error_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut record_field_exprs = vec![];
    match s.fields {
        syn::Fields::Named(ref a) => {
            let mut index: usize = 0;
            for field in a.named.iter() {
                let mut name = field.ident.as_ref().unwrap().to_string(); // we know everything has a name
                if let Some(raw_name) = name.strip_prefix("r#") {
                    name = raw_name.to_string();
                }
                let field_attrs =
                    FieldOptions::from_attributes(&field.attrs[..]).map_err(darling_to_syn)?;
                let doc = preserve_optional(field_attrs.doc);
                if let Some(rename) = field_attrs.rename {
                    name = rename
                }
                if let Some(true) = field_attrs.skip {
                    continue;
                }
                let default_value = match field_attrs.default {
                    Some(default_value) => {
                        let _: serde_json::Value = serde_json::from_str(&default_value[..])
                            .map_err(|e| {
                                vec![syn::Error::new(
                                    field.ident.span(),
                                    format!("Invalid avro default json: \n{e}"),
                                )]
                            })?;
                        quote! {
                            Some(serde_json::from_str(#default_value).expect(format!("Invalid JSON: {:?}", #default_value).as_str()))
                        }
                    }
                    None => quote! { None },
                };
                let schema_expr = type_to_schema_expr(&field.ty)?;
                let position = index;
                record_field_exprs.push(quote! {
                    apache_avro::schema::RecordField {
                            name: #name.to_string(),
                            doc: #doc,
                            default: #default_value,
                            schema: #schema_expr,
                            order: apache_avro::schema::RecordFieldOrder::Ascending,
                            position: #position,
                            custom_attributes: Default::default(),
                        }
                });
                index += 1;
            }
        }
        syn::Fields::Unnamed(_) => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroSchema derive does not work for tuple structs",
            )])
        }
        syn::Fields::Unit => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroSchema derive does not work for unit structs",
            )])
        }
    }
    let record_doc = preserve_optional(record_doc);
    let record_aliases = preserve_vec(aliases);
    Ok(quote! {
        let schema_fields = vec![#(#record_field_exprs),*];
        let name = apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse struct name for schema {}", #full_schema_name)[..]);
        let lookup: std::collections::BTreeMap<String, usize> = schema_fields
            .iter()
            .map(|field| (field.name.to_owned(), field.position))
            .collect();
        apache_avro::schema::Schema::Record {
            name,
            aliases: #record_aliases,
            doc: #record_doc,
            fields: schema_fields,
            lookup,
            attributes: Default::default(),
        }
    })
}

fn get_data_struct_value_defs(
    data_struct: &syn::DataStruct,
    error_span: Span,
) -> Result<(Vec<TokenStream>, Vec<TokenStream>), Vec<syn::Error>> {
    let fields = match data_struct.fields {
        syn::Fields::Named(ref named_fields) => named_fields,
        syn::Fields::Unnamed(_) => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroValue derive does not work for tuple structs",
            )])
        }
        syn::Fields::Unit => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroValue derive does not work for unit structs",
            )])
        }
    };
    let mut into_fields = vec![];
    let mut from_field_assigns = vec![];
    let mut index = 0_usize;
    for field in fields.named.iter() {
        let field_attrs =
            FieldOptions::from_attributes(&field.attrs[..]).map_err(darling_to_syn)?;
        let field_ident = field
            .ident
            .clone()
            .expect("AvroValue requires only named struct fields");
        let name = match field_attrs.rename {
            Some(name_attr) => name_attr,
            None => field_ident.to_string(),
        };
        let value_expr = type_to_value_expr(&field.ty)?;
        // for cases in which the field was skipped, we must have a field-value, so we assume that
        // the field implements default, and assign a default value to it. We skip incrementing
        // the index, here, since it is not included in the record field
        from_field_assigns.push(match field_attrs.skip {
            Some(true) => {
                quote!(#field_ident: Default::default(),);
                continue;
            }
            _ => data_struct_value_assignment(field, index)?,
        });
        into_fields.push(data_struct_value_record_field(
            name,
            &field_ident,
            value_expr,
        ));
        index += 1;
    }
    Ok((from_field_assigns, into_fields))
}

fn data_struct_value_record_field(
    name: String,
    field_ident: &proc_macro2::Ident,
    value_expr: ValueExpr,
) -> TokenStream {
    let value_ident = Some(proc_macro2::Ident::new("value", field_ident.span()));
    let record_value = data_struct_value_field_as_value(
        field_ident.clone(),
        value_ident,
        value_expr,
        field_ident.span(),
    );
    quote!((#name.to_string(), #record_value),)
}

fn data_struct_value_field_as_value(
    ident: proc_macro2::Ident,
    receiver_ident: Option<proc_macro2::Ident>,
    value_expr: ValueExpr,
    span: proc_macro2::Span,
) -> TokenStream {
    let ident = match receiver_ident {
        Some(receiver_ident) => quote!(#receiver_ident.#ident),
        None => quote!(#ident),
    };
    match value_expr {
        ValueExpr::Convertable(_) => quote!(apache_avro::types::Value::from(#ident)),
        ValueExpr::Option(value_variant) => {
            let inner_expr = data_struct_value_field_as_value(
                proc_macro2::Ident::new("item", span),
                None,
                *value_variant,
                span,
            );
            quote!(match #ident {
                Some(item) => apache_avro::types::Value::Union(1, Box::new(#inner_expr)),
                None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            })
        }
        ValueExpr::Array(value_variant) => {
            let inner_expr = data_struct_value_field_as_value(
                proc_macro2::Ident::new("item", span),
                None,
                *value_variant,
                span,
            );
            quote!(apache_avro::types::Value::Array(#ident.into_iter().map(|item| {
                #inner_expr
            }).collect::<Vec<apache_avro::types::Value>>()))
        }
        ValueExpr::Map(value_variant) => {
            let inner_expr = data_struct_value_field_as_value(
                proc_macro2::Ident::new("value", span),
                None,
                *value_variant,
                span,
            );
            quote!(apache_avro::types::Value::Map(#ident.into_iter().map(|(key, value)| {
                    (key, #inner_expr)
            }).collect::<HashMap<String, apache_avro::types::Value>>()))
        }
    }
}

fn data_struct_value_assignment(
    field: &Field,
    index: usize,
) -> Result<TokenStream, Vec<syn::Error>> {
    let field_ident = field.clone().ident.unwrap();
    let ident = proc_macro2::Ident::new("value", field.span());
    let value_expr = type_to_value_expr(&field.ty)?;
    let value_assignment =
        data_struct_value_field_as_assigned(quote!(#ident), None, value_expr, ident.span());
    Ok(quote!(#field_ident: match record_fields.get(#index) {
        Some((_, value)) => {
            #value_assignment
        }?,
        _ => return Err(Self::Error::GetField(stringify!(#field_ident).to_string())),
    },))
}

fn data_struct_value_field_as_assigned(
    ident: TokenStream,
    receiver_ident: Option<proc_macro2::Ident>,
    value_expr: ValueExpr,
    span: proc_macro2::Span,
) -> TokenStream {
    let ident = match receiver_ident {
        Some(receiver_ident) => quote!(#receiver_ident.#ident),
        None => quote!(#ident),
    };
    match value_expr {
        ValueExpr::Convertable(field_type) => quote!(#field_type::try_from(#ident.clone())),
        ValueExpr::Option(value_variant) => {
            let union_ident = proc_macro2::Ident::new("union_value", span);
            let value_expr = data_struct_value_field_as_assigned(
                quote!(*#union_ident),
                None,
                *value_variant,
                ident.span(),
            );
            quote!(match #ident.clone() {
                apache_avro::types::Value::Union(0, _) => None,
                apache_avro::types::Value::Union(1, union_value) => Some(#value_expr),
                apache_avro::types::Value::Union(union_index, _) => {
                    Err(Self::Error::GetUnionVariant{
                        index: union_index as i64,
                        num_variants: 2,
                    })?
                },
                _ => Err(Self::Error::GetUnionVariant{
                    index: -1,
                    num_variants: 2,
                })?,
            }
            .transpose())
        }
        ValueExpr::Array(value_variant) => {
            let var_ident = proc_macro2::Ident::new("item_value", span);
            let value_expr = data_struct_value_field_as_assigned(
                quote!(#var_ident),
                None,
                *value_variant,
                ident.span(),
            );
            quote!(match #ident {
                apache_avro::types::Value::Array(item_values) => item_values
                    .iter()
                    .map(|#var_ident| { #value_expr })
                    .collect::<Result<Vec<_>, Self::Error>>(),
                other_value => {
                    Err(Self::Error::GetArray{
                        expected: apache_avro::schema::SchemaKind::Array,
                        other: other_value.into()
                    })
                }
            })
        }
        ValueExpr::Map(value_variant) => {
            let var_ident = proc_macro2::Ident::new("item_value", span);
            let value_expr = data_struct_value_field_as_assigned(
                quote!(#var_ident),
                None,
                *value_variant,
                ident.span(),
            );
            quote!(match #ident {
                apache_avro::types::Value::Map(field_value_pairs) => field_value_pairs
                    .iter()
                    .map(|(item_key, item_value)| (
                        #value_expr.map(|value| (item_key.clone(), value))
                    )).collect::<Result<HashMap<_, _>, Self::Error>>(),
                other_value => Err(Self::Error::GetMap{
                    expected: apache_avro::schema::SchemaKind::Map,
                    other: other_value.into()
                }),
            })
        }
    }
}

fn get_data_enum_schema_def(
    full_schema_name: &str,
    doc: Option<String>,
    aliases: Vec<String>,
    e: &syn::DataEnum,
    error_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(doc);
    let enum_aliases = preserve_vec(aliases);
    if e.variants.iter().all(|v| syn::Fields::Unit == v.fields) {
        let symbols: Vec<String> = e
            .variants
            .iter()
            .map(|variant| variant.ident.to_string())
            .collect();
        Ok(quote! {
            apache_avro::schema::Schema::Enum {
                name: apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse enum name for schema {}", #full_schema_name)[..]),
                aliases: #enum_aliases,
                doc: #doc,
                symbols: vec![#(#symbols.to_owned()),*],
                attributes: Default::default(),
            }
        })
    } else {
        Err(vec![syn::Error::new(
            error_span,
            "AvroSchema derive does not work for enums with non unit structs",
        )])
    }
}

fn get_data_enum_value_defs(
    ident: &syn::Ident,
    e: &syn::DataEnum,
    error_span: Span,
) -> Result<(Vec<TokenStream>, Vec<TokenStream>), Vec<syn::Error>> {
    if !e.variants.iter().all(|v| syn::Fields::Unit == v.fields) {
        return Err(vec![syn::Error::new(
            error_span,
            "AvroValue derive does not work for enums with non unit structs",
        )]);
    }
    Ok(e.variants.iter().enumerate().fold(
        (vec![], vec![]),
        |(mut into_matches, mut from_matches), (index, variant)| {
            let index = index as u32;
            let variant_string = variant.ident.to_string();
            into_matches.push(quote!(
                #ident::#variant => {
                    apache_avro::types::Value::Enum(#index, #variant_string.to_string())
                }
            ));
            from_matches.push(quote!(
                apache_avro::types::Value::Enum(#index, _) => Ok(#ident::#variant)
            ));
            (into_matches, from_matches)
        },
    ))
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    if let Type::Path(p) = ty {
        let type_string = p.path.segments.last().unwrap().ident.to_string();

        let schema = match &type_string[..] {
            "bool" => quote! {apache_avro::schema::Schema::Boolean},
            "i8" | "i16" | "i32" | "u8" | "u16" => quote! {apache_avro::schema::Schema::Int},
            "u32" | "i64" => quote! {apache_avro::schema::Schema::Long},
            "f32" => quote! {apache_avro::schema::Schema::Float},
            "f64" => quote! {apache_avro::schema::Schema::Double},
            "String" | "str" => quote! {apache_avro::schema::Schema::String},
            "char" => {
                return Err(vec![syn::Error::new_spanned(
                    ty,
                    "AvroSchema: Cannot guarantee successful deserialization of this type",
                )])
            }
            "u64" => {
                return Err(vec![syn::Error::new_spanned(
                ty,
                "Cannot guarantee successful serialization of this type due to overflow concerns",
            )])
            } // Can't guarantee serialization type
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
        Err(vec![syn::Error::new_spanned(
            ty,
            format!("Unable to generate schema for type: {ty:?}"),
        )])
    }
}

/// A parsed union of the Avro value expressions to create the required serialization/deserializaion
/// patterns required for converting simple & complex field values
#[derive(Clone)]
enum ValueExpr {
    Convertable(syn::Type),
    Option(Box<ValueExpr>),
    Array(Box<ValueExpr>),
    Map(Box<ValueExpr>),
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_value_expr(ty: &Type) -> Result<ValueExpr, Vec<syn::Error>> {
    match ty {
        Type::Path(p) => {
            let path_segment = p.path.segments.last().unwrap();
            let type_string = &path_segment.ident.to_string()[..];
            let value = match type_string {
                "Option" => {
                    let inner_ty = get_path_inner_type(ty, path_segment, 0)?;
                    ValueExpr::Option(Box::new(type_to_value_expr(&inner_ty)?))
                }
                "Vec" => {
                    let inner_ty = get_path_inner_type(ty, path_segment, 0)?;
                    ValueExpr::Array(Box::new(type_to_value_expr(&inner_ty)?))
                }
                "HashMap" => {
                    let inner_ty = get_path_inner_type(ty, path_segment, 1)?;
                    ValueExpr::Map(Box::new(type_to_value_expr(&inner_ty)?))
                }
                "char" => {
                    return Err(vec![syn::Error::new_spanned(
                        ty,
                        "AvroValue: Cannot guarantee successful deserialization of this type",
                    )])
                }
                "u64" => {
                    return Err(vec![syn::Error::new_spanned(
                        ty,
                        "Cannot guarantee successful serialization of this type due to overflow concerns",
                    )])
                }
                _ => {
                    ValueExpr::Convertable(ty.clone())
                }
            };
            Ok(value)
        }
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!("Unable to generate value for type: {ty:?}"),
        )]),
    }
}

/// Generates the schema def expression for fully qualified type paths using the associated function
/// - `A -> <A as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt()`
/// - `A<T> -> <A<T> as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt()`
fn type_path_schema_expr(p: &TypePath) -> TokenStream {
    quote! {<#p as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}
}

/// Returns the generic type argument from a PathType at the specified index
fn get_path_inner_type(
    ty: &Type,
    path_segment: &PathSegment,
    index: usize,
) -> Result<Type, Vec<syn::Error>> {
    match path_segment.clone().arguments {
        syn::PathArguments::AngleBracketed(angle_bracket) => {
            let gen_args: Vec<GenericArgument> = angle_bracket
                .args
                .iter()
                .cloned()
                .filter(|arg| matches!(arg, syn::GenericArgument::Type(_)))
                .collect();
            match gen_args.get(index) {
                Some(syn::GenericArgument::Type(ty)) => Ok(ty.clone()),
                _ => Err(vec![syn::Error::new_spanned(
                    ty,
                    "TypePath has unexpected type arguments",
                )]),
            }
        }
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroValue Option type has unexpected type arguments",
        )]),
    }
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

fn extract_outer_doc(attributes: &[Attribute]) -> Option<String> {
    let doc = attributes
        .iter()
        .filter(|attr| attr.style == AttrStyle::Outer && attr.path.is_ident("doc"))
        .map(|attr| {
            let mut tokens = attr.tokens.clone().into_iter();
            tokens.next(); // skip the Punct
            let to_trim: &[char] = &['"', ' '];
            tokens
                .next() // use the Literal
                .unwrap()
                .to_string()
                .trim_matches(to_trim)
                .to_string()
        })
        .collect::<Vec<String>>()
        .join("\n");
    if doc.is_empty() {
        None
    } else {
        Some(doc)
    }
}

fn preserve_optional(op: Option<impl quote::ToTokens>) -> TokenStream {
    match op {
        Some(tt) => quote! {Some(#tt.into())},
        None => quote! {None},
    }
}

fn preserve_vec(op: Vec<impl quote::ToTokens>) -> TokenStream {
    let items: Vec<TokenStream> = op.iter().map(|tt| quote! {#tt.into()}).collect();
    if items.is_empty() {
        quote! {None}
    } else {
        quote! {Some(vec![#(#items),*])}
    }
}

fn darling_to_syn(e: darling::Error) -> Vec<syn::Error> {
    let msg = format!("{e}");
    let token_errors = e.write_errors();
    vec![syn::Error::new(token_errors.span(), msg)]
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
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn basic_case_value() {
        let test_struct = quote! {
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                assert!(derive_avro_value(&input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {:?}",
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
                "Failed to parse as derive input when it should be able to. Error: {:?}",
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
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn struct_with_optional() {
        let struct_with_optional = quote! {
            struct Test4 {
                a : Option<i32>
            }
        };
        match syn::parse2::<DeriveInput>(struct_with_optional) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {:?}",
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
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_basic_enum_value() {
        let basic_enum = quote! {
            enum Basic {
                A,
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(basic_enum) {
            Ok(input) => {
                assert!(derive_avro_value(&input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_non_basic_enum() {
        let non_basic_enum = quote! {
            enum Basic {
                A(i32),
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(non_basic_enum) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {:?}",
                error
            ),
        };
    }

    #[test]
    fn test_namespace() {
        let test_struct = quote! {
            #[avro(namespace = "namespace.testing")]
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
                "Failed to parse as derive input when it should be able to. Error: {:?}",
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

    #[test]
    fn test_trait_cast() {
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{i32}).unwrap()).to_string(), quote!{<i32 as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{Vec<T>}).unwrap()).to_string(), quote!{<Vec<T> as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{AnyType}).unwrap()).to_string(), quote!{<AnyType as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
    }
}
