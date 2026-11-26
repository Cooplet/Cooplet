use std::borrow::Cow;

use serde::{
    Deserialize,
    de::{DeserializeSeed, IntoDeserializer, MapAccess},
};

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct WireString<'a>(#[serde(borrow)] pub(crate) Cow<'a, str>);

// A buffered Serde discriminator can be a numeric variant index. Decode the tag
// as a JSON string while passing the remaining values directly to the derived map.
pub(crate) struct TaggedFields<M> {
    map: M,
    tag: &'static str,
    at_tag: bool,
}

impl<M> TaggedFields<M> {
    pub(crate) fn new(map: M, tag: &'static str) -> Self {
        Self {
            map,
            tag,
            at_tag: false,
        }
    }
}

impl<'de, M: MapAccess<'de>> MapAccess<'de> for TaggedFields<M> {
    type Error = M::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, M::Error> {
        let Some(WireString(key)) = self.map.next_key()? else {
            return Ok(None);
        };
        self.at_tag = key == self.tag;
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, M::Error> {
        if self.at_tag {
            let WireString(value) = self.map.next_value()?;
            seed.deserialize(value.into_deserializer())
        } else {
            self.map.next_value_seed(seed)
        }
    }
}

// Serde's default unit-enum decoder also accepts externally tagged objects.
// Use the same variant declaration for serialization, schema and string lookup.
macro_rules! wire_string_enum {
    ($(#[$attr:meta])* pub enum $name:ident $variants:tt) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
        $(#[$attr])*
        #[serde(rename_all = "snake_case")]
        pub enum $name $variants

        const _: () = {
            type Label = $name;

            #[allow(dead_code, reason = "Serde's remote derive maps wire names to existing variants")]
            #[derive(serde::Deserialize)]
            #[serde(remote = "Label", rename_all = "snake_case")]
            enum Variants $variants

            impl<'de> serde::Deserialize<'de> for Label {
                fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    let text = <$crate::wire::WireString as serde::Deserialize>::deserialize(deserializer)?;
                    Variants::deserialize(serde::de::IntoDeserializer::into_deserializer(text.0))
                }
            }
        };
    };
}

// Serde also accepts positional sequences for derived structs and tagged enums.
// A wire record always has named fields. Keep the derived map decoder for defaults,
// duplicate-field errors, borrowed data and the per-record unknown-field policy.
macro_rules! object_serde {
    ($(#[$attr:meta])* $kind:ident $name:ident $fields:tt $(tag = $tag:literal)?) => {
        const _: () = {
            type Record = $name;

            #[allow(dead_code, reason = "Serde's remote derive describes the existing record")]
            #[derive(serde::Serialize, serde::Deserialize)]
            #[serde(remote = "Record")]
            $(#[$attr])*
            $kind Fields $fields

            impl serde::Serialize for Record {
                fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    Fields::serialize(self, serializer)
                }
            }

            impl<'de> serde::Deserialize<'de> for Record {
                fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    struct NamedFields;

                    impl<'de> serde::de::Visitor<'de> for NamedFields {
                        type Value = Record;

                        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            write!(formatter, "a {} object", stringify!($name))
                        }

                        fn visit_map<M: serde::de::MapAccess<'de>>(self, map: M) -> Result<Record, M::Error> {
                            $(let map = $crate::wire::TaggedFields::new(map, $tag);)?
                            Fields::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                        }
                    }

                    deserializer.deserialize_map(NamedFields)
                }
            }
        };
    };
}

macro_rules! wire_object {
    ($(#[$attr:meta])* pub struct $name:ident $fields:tt) => {
        #[derive(Clone, Debug, PartialEq, Eq, utoipa::ToSchema)]
        $(#[$attr])*
        pub struct $name $fields

        $crate::wire::object_serde! { $(#[$attr])* struct $name $fields }
    };
    (#[serde(tag = $tag:tt, $($rule:tt)+)] pub enum $name:ident { $($variant:ident $fields:tt),+ $(,)? }) => {
        #[derive(Clone, Debug, PartialEq, Eq, utoipa::ToSchema)]
        #[serde(tag = $tag, $($rule)+)]
        pub enum $name {
            // Utoipa reads the variant's attributes when generating each oneOf object.
            $(#[serde(deny_unknown_fields)] $variant $fields,)+
        }

        $crate::wire::object_serde! {
            #[serde(tag = $tag, $($rule)+)]
            enum $name { $($variant $fields,)+ } tag = $tag
        }
    };
}

pub(crate) use {object_serde, wire_object, wire_string_enum};
