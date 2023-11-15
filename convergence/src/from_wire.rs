use bytes::Bytes;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use std::fmt::Debug;
use std::mem;
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
	#[error("{0}")]
	Size(String),

	#[error("Format Error")]
	Format(#[from] Utf8Error),

	#[error("Parse Error")]
	Parse(#[from] Box<dyn std::error::Error>),
}

#[derive(Debug)]
pub enum ParseNumberError {
	IntError(std::num::ParseIntError),
	FloatError(std::num::ParseFloatError),
}

impl From<std::num::ParseIntError> for ParseNumberError {
	fn from(error: std::num::ParseIntError) -> Self {
		ParseNumberError::IntError(error)
	}
}

impl From<std::num::ParseFloatError> for ParseNumberError {
	fn from(error: std::num::ParseFloatError) -> Self {
		ParseNumberError::FloatError(error)
	}
}

pub trait FromWire: Sized {
	fn from_binary(raw: &[u8]) -> Result<Self, ParseError>;

	fn from_text(raw: &[u8]) -> Result<Self, ParseError>;
}

impl FromWire for bool {
	fn from_binary(raw: &[u8]) -> Result<Self, ParseError> {
		if raw.len() != 1 {
			return Err(ParseError::Size("Invalid message size".into()));
		}
		Ok(raw[0] != 0)
	}

	fn from_text(raw: &[u8]) -> Result<Self, ParseError> {
		use ParseError::*;
		let as_str = std::str::from_utf8(raw).map_err(Format)?;
		Ok(as_str == "t" || as_str == "y")
	}
}

// impl FromWire for i16 {
// 	fn from_binary(raw: &[u8]) -> Result<Self, ParseError> {
// 		if raw.len() != 2 {
// 			return Err(ParseError::SizeError("Invalid message size".into()));
// 		}
// 		let v = BigEndian::read_i16(raw);
// 		Ok(v)
// 	}

// 	fn from_text(raw: &[u8]) -> Result<Self, ParseError> {
// 		use ParseError::*;
// 		let as_str = std::str::from_utf8(raw).map_err(FormatError)?;
// 		// as_str.parse::<i16>().map_err(FormatError)?
// 		Ok(1)
// 	}
// }

fn from_binary_i8(mut raw: &[u8]) -> Result<i8, ParseError> {
	use ParseError::*;
	raw.read_i8().map_err(|e| Parse(Box::new(e)))
}

fn from_binary_i16(raw: &[u8]) -> Result<i16, ParseError> {
	let v = BigEndian::read_i16(raw);
	Ok(v)
}

fn from_binary_i32(raw: &[u8]) -> Result<i32, ParseError> {
	let v = BigEndian::read_i32(raw);
	Ok(v)
}

fn from_binary_i64(raw: &[u8]) -> Result<i64, ParseError> {
	let v = BigEndian::read_i64(raw);
	Ok(v)
}

fn from_binary_f32(raw: &[u8]) -> Result<f32, ParseError> {
	let v = BigEndian::read_f32(raw);
	Ok(v)
}
fn from_binary_f64(raw: &[u8]) -> Result<f64, ParseError> {
	let v = BigEndian::read_f64(raw);
	Ok(v)
}

fn from_binary_u8(mut raw: &[u8]) -> Result<u8, ParseError> {
	use ParseError::*;
	raw.read_u8().map_err(|e| Parse(Box::new(e)))
}

fn from_binary_u16(raw: &[u8]) -> Result<u16, ParseError> {
	let v = BigEndian::read_u16(raw);
	Ok(v)
}

fn from_binary_u32(raw: &[u8]) -> Result<u32, ParseError> {
	let v = BigEndian::read_u32(raw);
	Ok(v)
}

macro_rules! from_wire {
	($type: ident, $fn: ident) => {
		#[allow(missing_docs)]
		impl FromWire for $type {
			fn from_binary(raw: &[u8]) -> Result<Self, ParseError> {
				const LEN: usize = mem::size_of::<$type>();
				if raw.len() != LEN {
					return Err(ParseError::Size("Invalid message size".into()));
				}
				$fn(raw)
			}

			fn from_text(raw: &[u8]) -> Result<Self, ParseError> {
				use ParseError::*;
				let as_str = std::str::from_utf8(raw).map_err(Format)?;

				as_str.parse::<$type>().map_err(|e| Parse(Box::new(e)))
			}
		}
	};
}

from_wire!(i8, from_binary_i8);
from_wire!(i16, from_binary_i16);
from_wire!(i32, from_binary_i32);
from_wire!(i64, from_binary_i64);
from_wire!(f32, from_binary_f32);
from_wire!(f64, from_binary_f64);
from_wire!(u8, from_binary_u8);
from_wire!(u16, from_binary_u16);
from_wire!(u32, from_binary_u32);

#[cfg(test)]
mod tests {
	use super::FromWire;
	use crate::to_wire::ToWire;
	use pretty_assertions::assert_eq;
	use rand::Rng;

	// #[test]
	// pub fn test_from_wire_binary_i16() {
	// 	let expected = 42;

	// 	let raw = expected.to_binary();

	// 	let result = i16::from_binary(&raw).unwrap();
	// 	assert_eq!(expected, result);
	// }

	macro_rules! test_from_wire_binary {
		($name: ident, $type: ident) => {
			#[test]
			pub fn $name() {
				let min: $type = 0 as $type;
				let max: $type = $type::MAX;

				let mut rng = rand::thread_rng();
				let expected: $type = rng.gen_range(min..max);
				let raw = expected.to_binary();

				let result = $type::from_binary(&raw).unwrap();
				assert_eq!(expected, result);

				let raw = &b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"[..];
				let result = $type::from_binary(&raw);
				assert!(result.is_err());
			}
		};
	}
	macro_rules! test_from_wire_text {
		($name: ident, $type: ident) => {
			#[test]
			pub fn $name() {
				let min: $type = 0 as $type;
				let max: $type = $type::MAX;

				let mut rng = rand::thread_rng();
				let expected: $type = rng.gen_range(min..max);
				let raw = expected.to_string().to_text();
				let result = $type::from_text(&raw).unwrap();
				assert_eq!(expected, result);
				println!("{} {}", expected, result);
			}
		};
	}
	test_from_wire_binary!(test_from_wire_binary_i8, i8);
	test_from_wire_binary!(test_from_wire_binary_i16, i16);
	test_from_wire_binary!(test_from_wire_binary_i32, i32);
	test_from_wire_binary!(test_from_wire_binary_i64, i64);
	test_from_wire_binary!(test_from_wire_binary_f32, f32);
	test_from_wire_binary!(test_from_wire_binary_f64, f64);

	test_from_wire_binary!(test_from_wire_binary_u8, u8);
	test_from_wire_binary!(test_from_wire_binary_u16, u16);
	test_from_wire_binary!(test_from_wire_binary_u32, u32);

	test_from_wire_text!(test_from_wire_text_i8, i8);
	test_from_wire_text!(test_from_wire_text_i16, i16);
	test_from_wire_text!(test_from_wire_text_i32, i32);
	test_from_wire_text!(test_from_wire_text_i64, i64);
	test_from_wire_text!(test_from_wire_text_f32, f32);
	test_from_wire_text!(test_from_wire_text_f64, f64);

	test_from_wire_text!(test_from_wire_text_u8, u8);
	test_from_wire_text!(test_from_wire_text_u16, u16);
	test_from_wire_text!(test_from_wire_text_u32, u32);

	#[test]
	pub fn test_from_wire_binary_bool() {
		let expected = true;
		let raw = &b"\x01"[..];
		let result = bool::from_binary(raw).unwrap();
		assert_eq!(expected, result);

		let expected = false;
		let raw = &b"\x00"[..];
		let result = bool::from_binary(raw).unwrap();
		assert_eq!(expected, result);

		let result = bool::from_binary(&[]);
		assert!(result.is_err());
	}

	#[test]
	pub fn test_from_wire_text_bool() {
		let expected = true;
		let raw = &b"t"[..];
		let result = bool::from_text(raw).unwrap();
		assert_eq!(expected, result);

		let expected = false;
		let raw = &b"f"[..];
		let result = bool::from_text(raw).unwrap();
		assert_eq!(expected, result);

		let raw: [u8; 1] = [159];
		let result = bool::from_text(&raw);
		assert!(result.is_err());
	}
}
