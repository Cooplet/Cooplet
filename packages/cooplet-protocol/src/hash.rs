use std::io;

use serde::{
    Serialize, Serializer,
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use crate::ProtocolError;

/// Compact JSON with object keys sorted recursively by UTF-8 byte order.
/// Array order is significant. Non-finite floats (`NaN`, `Infinity`) are rejected.
pub fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let value = value.serialize(CanonicalSerializer)?;
    Ok(serde_json::to_vec(&value)?)
}

/// SHA-256 hex digest of [`canonical_json_bytes`], streamed directly into the hasher.
pub fn canonical_sha256<T: Serialize + ?Sized>(value: &T) -> Result<String, ProtocolError> {
    let value = value.serialize(CanonicalSerializer)?;
    let mut writer = Sha256Writer(Sha256::new());
    serde_json::to_writer(&mut writer, &value)?;
    Ok(format!("{:x}", writer.0.finalize()))
}

struct Sha256Writer(Sha256);

impl io::Write for Sha256Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Single-pass O(N) Value serializer that rejects non-finite floats and sorts
// object keys in UTF-8 byte order regardless of serde_json's preserve_order feature.
struct CanonicalSerializer;

fn non_finite_error() -> serde_json::Error {
    ser::Error::custom("non-finite float is not valid canonical JSON")
}

fn non_string_key_error() -> serde_json::Error {
    ser::Error::custom("canonical JSON map key must be a string or integer")
}

impl Serializer for CanonicalSerializer {
    type Ok = Value;
    type Error = serde_json::Error;
    type SerializeSeq = CanonicalSeq;
    type SerializeTuple = CanonicalSeq;
    type SerializeTupleStruct = CanonicalSeq;
    type SerializeTupleVariant = CanonicalTupleVariant;
    type SerializeMap = CanonicalMap;
    type SerializeStruct = CanonicalMap;
    type SerializeStructVariant = CanonicalStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Bool(v))
    }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        v.serialize(serde_json::value::Serializer)
    }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Number(v.into()))
    }
    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        v.serialize(serde_json::value::Serializer)
    }
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Number::from_f64(f64::from(v))
            .map(Value::Number)
            .ok_or_else(non_finite_error)
    }
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(non_finite_error)
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.to_owned()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(
            v.iter().map(|&b| Value::Number(b.into())).collect(),
        ))
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(variant.to_owned()))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        let mut map = Map::new();
        map.insert(variant.to_owned(), value.serialize(self)?);
        Ok(Value::Object(map))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(CanonicalSeq(Vec::with_capacity(len.unwrap_or(0))))
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(CanonicalSeq(Vec::with_capacity(len)))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(CanonicalSeq(Vec::with_capacity(len)))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(CanonicalTupleVariant {
            variant,
            items: Vec::with_capacity(len),
        })
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(CanonicalMap {
            map: Map::new(),
            next_key: None,
        })
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(CanonicalMap {
            map: Map::new(),
            next_key: None,
        })
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(CanonicalStructVariant {
            variant,
            map: Map::new(),
        })
    }
}

struct CanonicalSeq(Vec<Value>);

impl SerializeSeq for CanonicalSeq {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.push(value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(self.0))
    }
}

impl SerializeTuple for CanonicalSeq {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.push(value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(self.0))
    }
}

impl SerializeTupleStruct for CanonicalSeq {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.push(value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(self.0))
    }
}

struct CanonicalTupleVariant {
    variant: &'static str,
    items: Vec<Value>,
}

impl SerializeTupleVariant for CanonicalTupleVariant {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.items.push(value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut map = Map::new();
        map.insert(self.variant.to_owned(), Value::Array(self.items));
        Ok(Value::Object(map))
    }
}

struct CanonicalMap {
    map: Map<String, Value>,
    next_key: Option<String>,
}

impl SerializeMap for CanonicalMap {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        let key = match key.serialize(CanonicalSerializer)? {
            Value::String(s) => s,
            Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
            _ => return Err(non_string_key_error()),
        };
        self.next_key = Some(key);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let key = self.next_key.take().ok_or_else(non_string_key_error)?;
        self.map.insert(key, value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.map.sort_keys();
        Ok(Value::Object(self.map))
    }
}

impl SerializeStruct for CanonicalMap {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.map
            .insert(key.to_owned(), value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.map.sort_keys();
        Ok(Value::Object(self.map))
    }
}

struct CanonicalStructVariant {
    variant: &'static str,
    map: Map<String, Value>,
}

impl SerializeStructVariant for CanonicalStructVariant {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.map
            .insert(key.to_owned(), value.serialize(CanonicalSerializer)?);
        Ok(())
    }
    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.map.sort_keys();
        let mut outer = Map::new();
        outer.insert(self.variant.to_owned(), Value::Object(self.map));
        Ok(Value::Object(outer))
    }
}
