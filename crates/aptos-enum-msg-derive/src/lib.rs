use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Meta, NestedMeta};

#[proc_macro_derive(EnumMessage, attributes(enum_variant))]
pub fn derive_enum_converters(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct type
    let struct_name = &input.ident;

    // Check if the input is a struct
    if let Data::Struct(_) = input.data {
        // Find the variant name attribute, if present
        let enum_name = find_enum_name(&input.attrs);

        // Generate the `From` implementation
        if let Some(variant) = enum_name {
            let enum_msg = quote::format_ident!("{}Msg", struct_name);
            let expanded = quote! {
                impl From<#struct_name> for #variant {
                    fn from(struct_value: #struct_name) -> Self {
                        Self::#enum_msg(struct_value)
                    }
                }

                impl TryFrom<#variant> for #struct_name {
                    type Error = anyhow::Error;

                    fn try_from(msg: #variant) -> Result<Self, Self::Error> {
                        match msg {
                            #variant::#enum_msg(m) => Ok(m),
                            _ => Err(anyhow::anyhow!("invalid message type")),
                        }
                    }
                }
            };

            return expanded.into();
        }
    }

    // If the input is not a valid struct, return a compilation error
    let error_message: &str = "This derive macro only supports structs with named fields";
    syn::Error::new_spanned(input, error_message)
        .to_compile_error()
        .into()
}

// Find the variant name specified in the attributes
fn find_enum_name(attrs: &[Attribute]) -> Option<syn::Ident> {
    for attr in attrs {
        if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
            if meta_list.path.is_ident("enum_variant") {
                if let Some(NestedMeta::Meta(Meta::Path(variant_path))) =
                    meta_list.nested.first()
                {
                    if let Some(segment) = variant_path.segments.last() {
                        return Some(segment.ident.clone());
                    }
                }
            }
        }
    }
    None
}
