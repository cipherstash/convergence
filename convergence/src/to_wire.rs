use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_types::ToSql;
use rust_decimal::Decimal;

pub trait Writer {
	fn write<T>(&mut self, val: T)
	where
		T: ToWire;

	fn write_nullable<T>(&mut self, val: Option<T>)
	where
		T: ToWire;
}

pub trait ToWire {
	fn to_binary(&self) -> Vec<u8>;

	fn to_text(&self) -> Vec<u8>;
}

impl ToWire for bool {
	fn to_binary(&self) -> Vec<u8> {
		(*self as u8).to_be_bytes().into()
	}
	fn to_text(&self) -> Vec<u8> {
		if *self { "t" } else { "f" }.as_bytes().into()
	}
}

impl ToWire for &str {
	fn to_binary(&self) -> Vec<u8> {
		self.as_bytes().into()
	}
	fn to_text(&self) -> Vec<u8> {
		self.as_bytes().into()
	}
}

fn pg_date_epoch() -> NaiveDate {
	NaiveDate::from_ymd_opt(2000, 1, 1).expect("failed to create pg date epoch")
}

fn pg_timestamp_epoch() -> NaiveDateTime {
	pg_date_epoch()
		.and_hms_opt(0, 0, 0)
		.expect("failed to create pg timestamp epoch")
}

impl ToWire for NaiveDate {
	fn to_binary(&self) -> Vec<u8> {
		let d: i32 = self.signed_duration_since(pg_date_epoch()).num_days() as i32;
		d.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for NaiveDateTime {
	fn to_binary(&self) -> Vec<u8> {
		let dt: i64 = self
			.signed_duration_since(pg_timestamp_epoch())
			.num_microseconds()
			.unwrap();
		dt.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for NaiveTime {
	fn to_binary(&self) -> Vec<u8> {
		let delta = self.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
		let t = delta.num_microseconds().unwrap_or(0);
		t.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for DateTime<Utc> {
	fn to_binary(&self) -> Vec<u8> {
		self.naive_utc().to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_rfc3339().as_bytes().into()
	}
}

impl ToWire for Decimal {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::NUMERIC, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for uuid::Uuid {
	fn to_binary(&self) -> Vec<u8> {
		self.as_bytes().to_vec()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for Vec<i16> {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::INT2_ARRAY, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		let s: Vec<String> = self.iter().map(|v| v.to_string()).collect();
		let s = format!("{{{}}}", s.join(","));
		s.to_string().as_bytes().into()
	}
}

impl ToWire for Vec<i32> {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::INT4_ARRAY, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		let s: Vec<String> = self.iter().map(|v| v.to_string()).collect();
		let s = format!("{{{}}}", s.join(","));
		s.to_string().as_bytes().into()
	}
}

impl ToWire for Vec<i64> {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::INT8_ARRAY, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		let s: Vec<String> = self.iter().map(|v| v.to_string()).collect();
		let s = format!("{{{}}}", s.join(","));
		s.to_string().as_bytes().into()
	}
}

impl ToWire for Vec<f32> {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::FLOAT4_ARRAY, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		let s: Vec<String> = self.iter().map(|v| v.to_string()).collect();
		let s = format!("{{{}}}", s.join(","));
		s.to_string().as_bytes().into()
	}
}

impl ToWire for Vec<f64> {
	fn to_binary(&self) -> Vec<u8> {
		let mut b = BytesMut::new();
		self.to_sql(&postgres_types::Type::FLOAT8_ARRAY, &mut b).unwrap();
		b.into()
	}
	fn to_text(&self) -> Vec<u8> {
		let s: Vec<String> = self.iter().map(|v| v.to_string()).collect();
		let s = format!("{{{}}}", s.join(","));
		s.to_string().as_bytes().into()
	}
}

macro_rules! to_wire {
	($type: ident) => {
		#[allow(missing_docs)]
		impl ToWire for $type {
			fn to_binary(&self) -> Vec<u8> {
				self.to_be_bytes().into()
			}
			fn to_text(&self) -> Vec<u8> {
				self.to_string().as_bytes().into()
			}
		}
	};
}

to_wire!(i8);
to_wire!(i16);
to_wire!(i32);
to_wire!(i64);
to_wire!(u8);
to_wire!(u16);
to_wire!(u32);
to_wire!(f32);
to_wire!(f64);

#[cfg(test)]
mod tests {
	use bytes::{BufMut, BytesMut};
	use chrono::NaiveDateTime;
	use postgres_types::{FromSql, Type};
	use rand::Rng;
	use std::{convert::TryInto, mem};

	use super::{ToWire, Writer};

	struct TestWriter {
		pub buf: BytesMut,
	}

	impl Writer for TestWriter {
		fn write<T>(&mut self, val: T)
		where
			T: ToWire,
		{
			self.buf.put_slice(&val.to_binary());
		}

		fn write_nullable<T>(&mut self, val: Option<T>)
		where
			T: ToWire,
		{
			match val {
				Some(val) => self.buf.put_slice(&val.to_binary()),
				None => self.buf.put_i32(-1),
			}
		}
	}

	#[test]
	pub fn test_timestamp() {
		let expected = "2023-10-21 04:34:48";
		let date = NaiveDateTime::from_timestamp_opt(1697862888, 0).unwrap();
		let out = date.to_text();

		let out = String::from_utf8(out).unwrap();

		assert_eq!(expected, out);
	}

	#[test]
	pub fn test_uuid() {
		let val = uuid::uuid!("9f398883-76cb-474b-b95b-877b8f7c5a27");

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "9f398883-76cb-474b-b95b-877b8f7c5a27";
		assert_eq!(from_text, expected_from_text);

		let from_binary = uuid::Uuid::from_sql(&Type::UUID, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	#[test]
	pub fn test_int2_array() {
		let val: Vec<i16> = vec![1024, 2048, 1234];

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "{1024,2048,1234}";
		assert_eq!(from_text, expected_from_text);

		let from_binary = Vec::<i16>::from_sql(&Type::INT2_ARRAY, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	#[test]
	pub fn test_int4_array() {
		let val: Vec<i32> = vec![1024, 2048, 1234];

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "{1024,2048,1234}";
		assert_eq!(from_text, expected_from_text);

		let from_binary = Vec::<i32>::from_sql(&Type::INT4_ARRAY, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	#[test]
	pub fn test_int8_array() {
		let val: Vec<i32> = vec![1024, 2048, 1234];

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "{1024,2048,1234}";
		assert_eq!(from_text, expected_from_text);

		let from_binary = Vec::<i32>::from_sql(&Type::INT8_ARRAY, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	#[test]
	pub fn test_float4_array() {
		let val: Vec<f32> = vec![1.024, 2.048, 1.234];

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "{1.024,2.048,1.234}";
		assert_eq!(from_text, expected_from_text);

		let from_binary = Vec::<f32>::from_sql(&Type::FLOAT4_ARRAY, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	#[test]
	pub fn test_float8_array() {
		let val: Vec<f64> = vec![1.024, 2.048, 1.234];

		let as_text = val.to_text();
		let from_text = String::from_utf8(as_text).unwrap();
		let expected_from_text = "{1.024,2.048,1.234}";
		assert_eq!(from_text, expected_from_text);

		let from_binary = Vec::<f64>::from_sql(&Type::FLOAT8_ARRAY, &val.to_binary()).unwrap();
		assert_eq!(from_binary, val);
	}

	macro_rules! test_to_wire {
		($name: ident, $type: ident) => {
			#[test]
			pub fn $name() {
				const LEN: usize = mem::size_of::<$type>();

				let min: $type = 0 as $type;
				let max: $type = $type::MAX;

				let mut rng = rand::thread_rng();
				let expected: $type = rng.gen_range(min..max);

				let val: $type = expected;

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write(val);

				let data: [u8; LEN] = w.buf[..LEN].try_into().expect("Expected $type");
				let out = $type::from_be_bytes(data);

				assert_eq!(expected, out);

				// Option<T>
				let val: Option<$type> = Some(expected);

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write_nullable(val);

				let data: [u8; LEN] = w.buf[..LEN].try_into().expect("Expected $type");
				let out = $type::from_be_bytes(data);

				assert_eq!(expected, out);
			}
		};
	}

	macro_rules! test_to_wire_null {
		($name: ident, $type: ident) => {
			#[test]
			pub fn $name() {
				// Option<T>
				let val: Option<$type> = None;

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write_nullable(val);

				let data: [u8; 4] = w.buf[..4].try_into().expect("Expected $type");
				let out = i32::from_be_bytes(data);

				let expected: i32 = -1;
				assert_eq!(expected, out);
			}
		};
	}

	test_to_wire!(test_to_wire_i16, i16);
	test_to_wire!(test_to_wire_i32, i32);
	test_to_wire!(test_to_wire_i64, i64);
	test_to_wire!(test_to_wire_f32, f32);
	test_to_wire!(test_to_wire_f64, f64);

	test_to_wire_null!(test_to_wire_null_i16, i16);
	test_to_wire_null!(test_to_wire_null_i32, i32);
	test_to_wire_null!(test_to_wire_null_i64, i64);
	test_to_wire_null!(test_to_wire_null_f32, f32);
	test_to_wire_null!(test_to_wire_null_f64, f64);
}
