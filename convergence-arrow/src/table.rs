use arrow::array::{
	Date32Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
	TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
	UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use convergence::protocol::{DataTypeOid, FieldDescription};
use convergence::protocol_ext::DataRowBatch;

macro_rules! array_cast {
	($arrtype: ident, $arr: expr) => {
		$arr.as_any().downcast_ref::<$arrtype>().expect("array cast failed")
	};
}

macro_rules! array_val {
	($arrtype: ident, $arr: expr, $idx: expr, $func: ident) => {
		array_cast!($arrtype, $arr).$func($idx)
	};
	($arrtype: ident, $arr: expr, $idx: expr) => {
		array_val!($arrtype, $arr, $idx, value)
	};
}

pub fn record_batch_to_rows(arrow_batch: &RecordBatch, pg_batch: &mut DataRowBatch) {
	for row_idx in 0..arrow_batch.num_rows() {
		let mut row = pg_batch.create_row();
		for col_idx in 0..arrow_batch.num_columns() {
			let col = arrow_batch.column(col_idx);
			if col.is_null(row_idx) {
				row.write_null();
			} else {
				match col.data_type() {
					DataType::Int8 => row.write_int2(array_val!(Int8Array, col, row_idx) as i16),
					DataType::Int16 => row.write_int2(array_val!(Int16Array, col, row_idx)),
					DataType::Int32 => row.write_int4(array_val!(Int32Array, col, row_idx)),
					DataType::Int64 => row.write_int8(array_val!(Int64Array, col, row_idx)),
					DataType::UInt8 => row.write_int2(array_val!(UInt8Array, col, row_idx) as i16),
					DataType::UInt16 => row.write_int2(array_val!(UInt16Array, col, row_idx) as i16),
					DataType::UInt32 => row.write_int4(array_val!(UInt32Array, col, row_idx) as i32),
					DataType::UInt64 => row.write_int8(array_val!(UInt64Array, col, row_idx) as i64),
					DataType::Float32 => row.write_float4(array_val!(Float32Array, col, row_idx)),
					DataType::Float64 => row.write_float8(array_val!(Float64Array, col, row_idx)),
					DataType::Utf8 => row.write_string(array_val!(StringArray, col, row_idx)),
					DataType::Date32 => {
						row.write_date(array_val!(Date32Array, col, row_idx, value_as_date).expect("invalid date"))
					}
					DataType::Timestamp(unit, None) => row.write_timestamp(
						match unit {
							TimeUnit::Second => array_val!(TimestampSecondArray, col, row_idx, value_as_datetime),
							TimeUnit::Millisecond => {
								array_val!(TimestampMillisecondArray, col, row_idx, value_as_datetime)
							}
							TimeUnit::Microsecond => {
								array_val!(TimestampMicrosecondArray, col, row_idx, value_as_datetime)
							}
							TimeUnit::Nanosecond => {
								array_val!(TimestampNanosecondArray, col, row_idx, value_as_datetime)
							}
						}
						.expect("invalid timestamp"),
					),
					_ => unimplemented!(),
				};
			}
		}
	}
}

pub fn data_type_to_oid(ty: &DataType) -> DataTypeOid {
	match ty {
		DataType::Int8 | DataType::Int16 => DataTypeOid::Int2,
		DataType::Int32 => DataTypeOid::Int4,
		DataType::Int64 => DataTypeOid::Int8,
		// TODO: need to figure out a sensible mapping for unsigned
		DataType::UInt8 | DataType::UInt16 => DataTypeOid::Int2,
		DataType::UInt32 => DataTypeOid::Int4,
		DataType::UInt64 => DataTypeOid::Int8,
		// TODO: DataType::Float16 exists and could be mapped to Float4, but there's no Float16Array
		DataType::Float32 => DataTypeOid::Float4,
		DataType::Float64 => DataTypeOid::Float8,
		DataType::Utf8 => DataTypeOid::Text,
		DataType::Date32 => DataTypeOid::Date,
		DataType::Timestamp(_, None) => DataTypeOid::Timestamp,
		other => unimplemented!("arrow to pg conversion not implemented: {}", other),
	}
}

pub fn schema_to_field_desc(schema: &Schema) -> Vec<FieldDescription> {
	schema
		.fields()
		.iter()
		.map(|f| FieldDescription {
			name: f.name().clone(),
			data_type: data_type_to_oid(f.data_type()),
		})
		.collect()
}
