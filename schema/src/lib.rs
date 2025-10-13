use proc_macro::TokenStream;
use quote::{quote,format_ident};
use syn::{parse_macro_input, DeriveInput,Data,Fields,Attribute,Meta};

#[proc_macro_derive(Schema,attributes(index))]
pub fn derive_schema(inout:TokenStream)-> TokenStream{
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let (fields_checks,index_fields) = extarct_fields(&input);
    
    let expanded = quote!{
        impl rust_db_core::Schema for #name{
            fn validate(&self)->rust_db_core::Result<()>{
                #(#fields_checks)*
                Ok(())
            }

            fn table_name()-> &'static str{
                stringify!(#name)
            }

            fn indexes(&self) -> std:collections::HashMap<String,Vec<u8>>{

            }
        }
    };
    TokenStream::from(expanded)
}

fn extarct_fields(input: &DeriveInput) -> (Vec<proc_macro2::TokenStream>,Vec<proc_macro2::TokenStream>){
    let mut field_checks= Vec::new();
    let mut index_checks = Vec::new();

    if let Data::Struct(data) = &input.data{
        if let Fields::Named(fields) = &data.fields{
            for field in &fields.named{
                let field_name = field.ident.as_ref().unwrap();
                let field_name_str = field_name.to_string();

                field_checks.push(quote!{

                });

                for attr in &fields.attrs{
                    if attr.path().is_ident("index"){
                        index.fields.push(quote!{
                            indexes.insert(
                                #field_name_str.to_string(),
                                bincode::serialize(&self.#field_name).unwrap()
                            );
                        });
                    }
                }
            }
        }
    }
    (field_checks,index_fields)
}