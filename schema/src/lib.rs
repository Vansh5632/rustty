use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields};

#[proc_macro_derive(Schema, attributes(index))]
pub fn derive_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let (field_checks, index_fields, field_accessors) = extract_fields(&input);
    
    let expanded = quote! {
        // --- IMPL BLOCK 1: Schema ---
        impl rust_db_core::Schema for #name {
            fn validate(&self) -> rust_db_core::Result<()> {
                #(#field_checks)*
                Ok(())
            }

            fn table_name() -> &'static str {
                stringify!(#name)
            }

            fn indexes(&self) -> std::collections::HashMap<String, Vec<u8>> {
                let mut indexes = std::collections::HashMap::new();
                #(#index_fields)*
                indexes
            }
        }

        // --- IMPL BLOCK 2: FieldAccess ---
        impl rust_db_core::FieldAccess for #name {
            fn get_field(&self, field_name: &str) -> Option<rust_db_core::Value> {
                match field_name {
                    #(#field_accessors)*
                    _ => None,
                }
            }
        }

        // --- IMPL BLOCK 3-6: From implementations for Value conversion ---
        impl From<&u64> for rust_db_core::Value {
            fn from(val: &u64) -> Self {
                rust_db_core::Value::Int(*val as i64)
            }
        }

        impl From<&u32> for rust_db_core::Value {
            fn from(val: &u32) -> Self {
                rust_db_core::Value::Int(*val as i64)
            }
        }

        impl From<&f64> for rust_db_core::Value {
            fn from(val: &f64) -> Self {
                rust_db_core::Value::Float(*val)
            }
        }

        impl From<&bool> for rust_db_core::Value {
            fn from(val: &bool) -> Self {
                rust_db_core::Value::Bool(*val)
            }
        }

        impl From<&String> for rust_db_core::Value {
            fn from(val: &String) -> Self {
                rust_db_core::Value::String(val.clone())
            }
        }
    };
    TokenStream::from(expanded)
}

fn extract_fields(
    input: &DeriveInput,
) -> (
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
) {
    let mut field_checks = Vec::new();
    let mut index_fields = Vec::new();
    let mut field_accessors = Vec::new();

    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for field in &fields.named {
                let field_name = field.ident.as_ref().unwrap();
                let field_name_str = field_name.to_string();

                // Snippet for FieldAccess
                field_accessors.push(quote! {
                    #field_name_str => Some(rust_db_core::Value::from(&self.#field_name)),
                });

                // Snippet for Schema::indexes
                for attr in &field.attrs {
                    if attr.path().is_ident("index") {
                        index_fields.push(quote! {
                            indexes.insert(
                                #field_name_str.to_string(),
                                bincode::serialize(&self.#field_name).unwrap()
                            );
                        });
                    }
                }
                
                // Snippet for Schema::validate (placeholder)
                field_checks.push(quote! {
                    // Placeholder for field validation
                });
            }
        }
    }
    
    (field_checks, index_fields, field_accessors)
}