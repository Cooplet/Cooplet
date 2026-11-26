// Serde also accepts positional sequences for derived structs and tagged enums.
// A wire record always has named fields. Keep the derived map decoder for defaults,
// duplicate-field errors, borrowed data and the per-record unknown-field policy.
macro_rules! object_serde {
    ($(#[$attr:meta])* $kind:ident $name:ident $fields:tt) => {
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
    ($(#[$attr:meta])* pub enum $name:ident { $($variant:ident $fields:tt),+ $(,)? }) => {
        #[derive(Clone, Debug, PartialEq, Eq, utoipa::ToSchema)]
        $(#[$attr])*
        pub enum $name {
            // Utoipa reads the variant's attributes when generating each oneOf object.
            $(#[serde(deny_unknown_fields)] $variant $fields,)+
        }

        $crate::wire::object_serde! { $(#[$attr])* enum $name { $($variant $fields,)+ } }
    };
}

pub(crate) use {object_serde, wire_object};
