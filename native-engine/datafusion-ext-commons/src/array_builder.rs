// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::*;
use arrow::datatypes::*;
use arrow::datatypes::DataType::Struct;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use paste::paste;
use std::any::Any;
use std::sync::Arc;

pub fn new_array_builders(
    schema: &SchemaRef,
    batch_size: usize,
) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| new_array_builder(field.data_type(), batch_size))
        .collect::<Vec<_>>()
}

pub fn make_batch(
    schema: SchemaRef,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}

pub fn builder_extend(
    builder: &mut (impl ArrayBuilder + ?Sized),
    array: &ArrayRef,
    indices: &[usize],
    data_type: &DataType,
) {
    macro_rules! append_simple {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }
    macro_rules! append_decimal {
        ($builderty:ident, $arrowty:ident) => {{
            type B = paste::paste! {[< $builderty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_dict {
        ($key_type:expr, $value_type:expr) => {{
            append_dict!(@match_key: $key_type, $value_type)
        }};
        (@match_key: $key_type:expr, $value_type:expr) => {{
            match $key_type.as_ref() {
                DataType::Int8 => append_dict!(@match_value: Int8, $value_type),
                DataType::Int16 => append_dict!(@match_value: Int16, $value_type),
                DataType::Int32 => append_dict!(@match_value: Int32, $value_type),
                DataType::Int64 => append_dict!(@match_value: Int64, $value_type),
                DataType::UInt8 => append_dict!(@match_value: UInt8, $value_type),
                DataType::UInt16=> append_dict!(@match_value: UInt16, $value_type),
                DataType::UInt32 => append_dict!(@match_value: UInt32, $value_type),
                DataType::UInt64 => append_dict!(@match_value: UInt64, $value_type),
                _ => unimplemented!("dictionary key type not supported: {:?}", $value_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type.as_ref() {
                DataType::Int8 => append_dict!(@prim: $keyarrowty, Int8),
                DataType::Int16 => append_dict!(@prim: $keyarrowty, Int16),
                DataType::Int32 => append_dict!(@prim: $keyarrowty, Int32),
                DataType::Int64 => append_dict!(@prim: $keyarrowty, Int64),
                DataType::UInt8 => append_dict!(@prim: $keyarrowty, UInt8),
                DataType::UInt16 => append_dict!(@prim: $keyarrowty, UInt16),
                DataType::UInt32 => append_dict!(@prim: $keyarrowty, UInt32),
                DataType::UInt64 => append_dict!(@prim: $keyarrowty, UInt64),
                DataType::Float32 => append_dict!(@prim: $keyarrowty, Float32),
                DataType::Float64 => append_dict!(@prim: $keyarrowty, Float64),
                DataType::Date32 => append_dict!(@prim: $keyarrowty, Date32),
                DataType::Date64 => append_dict!(@prim: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_dict!(@prim: $keyarrowty, TimestampMicrosecond)
                }
                DataType::Utf8 => append_dict!(@str: $keyarrowty, i32),
                DataType::LargeUtf8 => append_dict!(@str: $keyarrowty, i64),
                _ => unimplemented!("dictionary value type not supported: {:?}", $value_type),
            }
        }};
        (@prim: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type ValueType = paste! {[< $valuearrowty Type >]};
            type B = PrimitiveDictionaryBuilder<KeyType, ValueType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<paste! {[<$valuearrowty Array>]} >().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
        (@bin: $keyarrowty:ident, $strsizety:ty) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type B = BinaryDictionaryBuilder<KeyType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<GenericStringArray<$strsizety>>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
        (@str: $keyarrowty:ident, $strsizety:ty) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type B = StringDictionaryBuilder<KeyType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<GenericStringArray<$strsizety>>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_list {
        ($data_type:expr) => {{
            append_list!(@match_type: $data_type)
        }};
        (@match_type: $data_type:expr) => {{
            match $data_type {
                DataType::Int8 => append_list!(@prim: Int8),
                DataType::Int16 => append_list!(@prim: Int16),
                DataType::Int32 => append_list!(@prim: Int32),
                DataType::Int64 => append_list!(@prim: Int64),
                DataType::UInt8 => append_list!(@prim: UInt8),
                DataType::UInt16 => append_list!(@prim: UInt16),
                DataType::UInt32 => append_list!(@prim: UInt32),
                DataType::UInt64 => append_list!(@prim: UInt64),
                DataType::Float32 => append_list!(@prim: Float32),
                DataType::Float64 => append_list!(@prim: Float64),
                DataType::Date32 => append_list!(@prim: Date32),
                DataType::Date64 => append_list!(@prim: Date64),
                DataType::Boolean => append_list!(@prim: Boolean),
                DataType::Utf8 => append_list!(@prim: String),
                DataType::LargeUtf8 => append_list!(@prim: LargeString),
                DataType::Timestamp(TimeUnit::Second, _) => append_list!(@prim: TimestampSecond),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    append_list!(@prim: TimestampMillisecond)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_list!(@prim: TimestampMicrosecond)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    append_list!(@prim: TimestampNanosecond)
                }
                DataType::Time32(TimeUnit::Second) => append_list!(@prim: Time32Second),
                DataType::Time32(TimeUnit::Millisecond) => append_list!(@prim: Time32Millisecond),
                DataType::Time64(TimeUnit::Microsecond) => append_list!(@prim: Time64Microsecond),
                DataType::Time64(TimeUnit::Nanosecond) => append_list!(@prim: Time64Nanosecond),
                DataType::Binary => append_list!(@prim: Binary),
                DataType::LargeBinary => append_list!(@prim: LargeBinary),
                DataType::Decimal128(_, _) => append_list!(@prim: ConfiguredDecimal128),
                DataType::Decimal256(_, _) => append_list!(@prim: ConfiguredDecimal256),
                _ => unimplemented!("list type not supported: {:?}", $data_type),
            }
        }};
        (@prim: $arrowty:ident) => {{
            type ElementType = paste! {[< $arrowty Builder >]};
            type B = ListBuilder<ElementType>;
            type A = ListArray;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    builder_extend(t.values(),&f.value(i),&(0..f.value(i).len()).collect::<Vec<_>>(), f.value(i).data_type());
                    t.append(true);
                } else {
                    t.append(false);
                }
            }
        }};
    }

    macro_rules! append_struct {
        ($fields:expr) => {{
            append_struct!(@make: $fields)
        }};
        (@make: $fields:expr) => {{
            type B = StructBuilder;
            type A = StructArray;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();

            for &i in indices {
                if f.is_valid(i) {
                    for j in 0..$fields.len() {
                        let field_builders = unsafe {
                             struct XNullBufferBuilder {
                                _bitmap_builder: Option<BooleanBufferBuilder>,
                                _len: usize,
                                _capacity: usize,
                            }
                            struct XStructBuilder {
                            _fields: Vec<Field>,
                            field_builders: Vec<Box<dyn ArrayBuilder>>,
                            _null_buffer_builder: XNullBufferBuilder,
                            }
                            let t: &mut XStructBuilder = std::mem::transmute(&mut (*t));
                            std::slice::from_raw_parts_mut(t.field_builders.as_mut_ptr(), t.field_builders.len())
                        };

                        builder_extend(field_builders[j].as_mut(), &f.column(j), &[i], $fields[j].data_type());
                    }
                    t.append(true);
                }
                else {
                    builder_append_null(t, &Struct($fields));
                }

            }
        }};
    }

    match data_type {
        DataType::Null => {
            builder
                .as_any_mut()
                .downcast_mut::<NullBuilder>()
                .unwrap()
                .extend(indices.len());
        }
        DataType::Boolean => append_simple!(Boolean),
        DataType::Int8 => append_simple!(Int8),
        DataType::Int16 => append_simple!(Int16),
        DataType::Int32 => append_simple!(Int32),
        DataType::Int64 => append_simple!(Int64),
        DataType::UInt8 => append_simple!(UInt8),
        DataType::UInt16 => append_simple!(UInt16),
        DataType::UInt32 => append_simple!(UInt32),
        DataType::UInt64 => append_simple!(UInt64),
        DataType::Float32 => append_simple!(Float32),
        DataType::Float64 => append_simple!(Float64),
        DataType::Date32 => append_simple!(Date32),
        DataType::Date64 => append_simple!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => append_simple!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            append_simple!(TimestampMillisecond)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_simple!(TimestampMicrosecond)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            append_simple!(TimestampNanosecond)
        }
        DataType::Time32(TimeUnit::Second) => append_simple!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append_simple!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append_simple!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append_simple!(Time64Nanosecond),
        DataType::Binary => append_simple!(Binary),
        DataType::LargeBinary => append_simple!(LargeBinary),
        DataType::Utf8 => append_simple!(String),
        DataType::LargeUtf8 => append_simple!(LargeString),
        DataType::Decimal128(_, _) => append_decimal!(ConfiguredDecimal128, Decimal128),
        DataType::Decimal256(_, _) => append_decimal!(ConfiguredDecimal256, Decimal256),
        DataType::Dictionary(key_type, value_type) => append_dict!(key_type, value_type),
        DataType::List(fields) => append_list!(fields.data_type()),
        DataType::Struct(fields) => append_struct!(fields.to_vec()),
        dt => unimplemented!("data type not supported in builder_extend: {:?}", dt),
    }
}

pub fn builder_append_null(to: &mut (impl ArrayBuilder + ?Sized), data_type: &DataType) {
    macro_rules! append {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            t.append_null();
        }};
    }

    macro_rules! append_null_for_list {
        ($data_type:expr) => {{
            append_null_for_list!(@match_type: $data_type)
        }};
        (@match_type: $data_type:expr) => {{
            match $data_type {
                DataType::Int8 => append_null_for_list!(@prim: Int8),
                DataType::Int16 => append_null_for_list!(@prim: Int16),
                DataType::Int32 => append_null_for_list!(@prim: Int32),
                DataType::Int64 => append_null_for_list!(@prim: Int64),
                DataType::UInt8 => append_null_for_list!(@prim: UInt8),
                DataType::UInt16 => append_null_for_list!(@prim: UInt16),
                DataType::UInt32 => append_null_for_list!(@prim: UInt32),
                DataType::UInt64 => append_null_for_list!(@prim: UInt64),
                DataType::Float32 => append_null_for_list!(@prim: Float32),
                DataType::Float64 => append_null_for_list!(@prim: Float64),
                DataType::Date32 => append_null_for_list!(@prim: Date32),
                DataType::Date64 => append_null_for_list!(@prim: Date64),
                DataType::Boolean => append_null_for_list!(@prim: Boolean),
                DataType::Utf8 => append_null_for_list!(@prim: String),
                DataType::LargeUtf8 => append_null_for_list!(@prim: LargeString),
                DataType::Timestamp(TimeUnit::Second, _) => append_null_for_list!(@prim: TimestampSecond),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    append_null_for_list!(@prim: TimestampMillisecond)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_null_for_list!(@prim: TimestampMicrosecond)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    append_null_for_list!(@prim: TimestampNanosecond)
                }
                DataType::Time32(TimeUnit::Second) => append_null_for_list!(@prim: Time32Second),
                DataType::Time32(TimeUnit::Millisecond) => append_null_for_list!(@prim: Time32Millisecond),
                DataType::Time64(TimeUnit::Microsecond) => append_null_for_list!(@prim: Time64Microsecond),
                DataType::Time64(TimeUnit::Nanosecond) => append_null_for_list!(@prim: Time64Nanosecond),
                DataType::Binary => append_null_for_list!(@prim: Binary),
                DataType::LargeBinary => append_null_for_list!(@prim: LargeBinary),
                DataType::Decimal128(_, _) => append_null_for_list!(@prim: ConfiguredDecimal128),
                DataType::Decimal256(_, _) => append_null_for_list!(@prim: ConfiguredDecimal256),
                _ => unimplemented!("list type not supported: {:?}", $data_type),
            }
        }};
        (@prim: $arrowty:ident) => {{
            type ElementType = paste! {[< $arrowty Builder >]};
            type B = ListBuilder<ElementType>;
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            t.append(false);
        }};
}

    macro_rules! append_null_for_struct {
        ($fields:expr) => {{
            append_null_for_struct!(@make: $fields)
        }};
        (@make: $fields:expr) => {{
            type B = StructBuilder;
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            for j in 0..$fields.len() {
                let field_builders = unsafe {
                     struct XNullBufferBuilder {
                        _bitmap_builder: Option<BooleanBufferBuilder>,
                        _len: usize,
                        _capacity: usize,
                    }
                    struct XStructBuilder {
                    _fields: Vec<Field>,
                    field_builders: Vec<Box<dyn ArrayBuilder>>,
                    _null_buffer_builder: XNullBufferBuilder,
                    }
                    let t: &mut XStructBuilder = std::mem::transmute(&mut (*t));
                    std::slice::from_raw_parts_mut(t.field_builders.as_mut_ptr(), t.field_builders.len())
                };
                builder_append_null(field_builders[j].as_mut(), $fields[j].data_type());
            }
            t.append_null();
        }};
    }

    match data_type {
        DataType::Null => {
            to.as_any_mut()
                .downcast_mut::<NullBuilder>()
                .unwrap()
                .append();
        }
        DataType::Boolean => append!(Boolean),
        DataType::Int8 => append!(Int8),
        DataType::Int16 => append!(Int16),
        DataType::Int32 => append!(Int32),
        DataType::Int64 => append!(Int64),
        DataType::UInt8 => append!(UInt8),
        DataType::UInt16 => append!(UInt16),
        DataType::UInt32 => append!(UInt32),
        DataType::UInt64 => append!(UInt64),
        DataType::Float32 => append!(Float32),
        DataType::Float64 => append!(Float64),
        DataType::Date32 => append!(Date32),
        DataType::Date64 => append!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => append!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => append!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => append!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => append!(TimestampNanosecond),
        DataType::Time32(TimeUnit::Second) => append!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append!(Time64Nanosecond),
        DataType::Binary => append!(Binary),
        DataType::LargeBinary => append!(LargeBinary),
        DataType::Utf8 => append!(String),
        DataType::LargeUtf8 => append!(LargeString),
        DataType::Decimal128(_, _) => append!(ConfiguredDecimal128),
        DataType::Decimal256(_, _) => append!(ConfiguredDecimal256),
        DataType::List(field) => append_null_for_list!(field.data_type()),
        DataType::Struct(fields) => append_null_for_struct!(fields),
        dt => unimplemented!("data type not supported in builder_append_null: {:?}", dt),
    }
}

fn new_array_builder(dt: &DataType, batch_size: usize) -> Box<dyn ArrayBuilder> {
    macro_rules! make_dictionary_builder {
        ($key_type:expr, $value_type:expr) => {{
            make_dictionary_builder!(@match_key: $key_type, $value_type)
        }};
        (@match_key: $key_type:expr, $value_type:expr) => {{
            match $key_type.as_ref() {
                DataType::Int8 => make_dictionary_builder!(@match_value: Int8, $value_type),
                DataType::Int16 => make_dictionary_builder!(@match_value: Int16, $value_type),
                DataType::Int32 => make_dictionary_builder!(@match_value: Int32, $value_type),
                DataType::Int64 => make_dictionary_builder!(@match_value: Int64, $value_type),
                DataType::UInt8 => make_dictionary_builder!(@match_value: UInt8, $value_type),
                DataType::UInt16 => make_dictionary_builder!(@match_value: UInt16, $value_type),
                DataType::UInt32 => make_dictionary_builder!(@match_value: UInt32, $value_type),
                DataType::UInt64 => make_dictionary_builder!(@match_value: UInt64, $value_type),
                _ => unimplemented!("unsupported dictionary key type: {:?}", $key_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type.as_ref() {
                DataType::Int8 => make_dictionary_builder!(@make: $keyarrowty, Int8),
                DataType::Int16 => make_dictionary_builder!(@make: $keyarrowty, Int16),
                DataType::Int32 => make_dictionary_builder!(@make: $keyarrowty, Int32),
                DataType::Int64 => make_dictionary_builder!(@make: $keyarrowty, Int64),
                DataType::UInt8 => make_dictionary_builder!(@make: $keyarrowty, UInt8),
                DataType::UInt16 => make_dictionary_builder!(@make: $keyarrowty, UInt16),
                DataType::UInt32 => make_dictionary_builder!(@make: $keyarrowty, UInt32),
                DataType::UInt64 => make_dictionary_builder!(@make: $keyarrowty, UInt64),
                DataType::Float32 => make_dictionary_builder!(@make: $keyarrowty, Float32),
                DataType::Float64 => make_dictionary_builder!(@make: $keyarrowty, Float64),
                DataType::Date32 => make_dictionary_builder!(@make: $keyarrowty, Date32),
                DataType::Date64 => make_dictionary_builder!(@make: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    make_dictionary_builder!(@make: $keyarrowty, TimestampMicrosecond)
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    make_dictionary_builder!(@make_str: $keyarrowty)
                }
                _ => unimplemented!("dictionary value type not supported: {:?}", $value_type),
            }
        }};
        (@make: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type ValueType = paste! {[< $valuearrowty Type >]};
            Box::new(PrimitiveDictionaryBuilder::<KeyType, ValueType>::new())
        }};
        (@make_str: $keyarrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            Box::new(StringDictionaryBuilder::<KeyType>::new())
        }};
    }

    macro_rules! make_list_builder {
            ($data_type:expr) => {{
                make_list_builder!(@match_type: $data_type)
            }};
            (@match_type: $data_type:expr) => {{
                match $data_type {
                    DataType::Int8 => make_list_builder!(@make: Int8),
                    DataType::Int16 => make_list_builder!(@make: Int16),
                    DataType::Int32 => make_list_builder!(@make: Int32),
                    DataType::Int64 => make_list_builder!(@make: Int64),
                    DataType::UInt8 => make_list_builder!(@make: UInt8),
                    DataType::UInt16 => make_list_builder!(@make: UInt16),
                    DataType::UInt32 => make_list_builder!(@make: UInt32),
                    DataType::UInt64 => make_list_builder!(@make: UInt64),
                    DataType::Float32 => make_list_builder!(@make: Float32),
                    DataType::Float64 => make_list_builder!(@make: Float64),
                    DataType::Date32 => make_list_builder!(@make: Date32),
                    DataType::Date64 => make_list_builder!(@make: Date64),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => make_list_builder!(@make: TimestampMicrosecond),
                    DataType::Boolean => make_list_builder!(@make: Boolean),
                    DataType::Utf8 => make_list_builder!(@make: String),
                    DataType::LargeUtf8 => make_list_builder!(@make: LargeString),
                    DataType::Binary => make_list_builder!(@make: Binary),
                    DataType::LargeBinary => make_list_builder!(@make: LargeBinary),
                    _ => unimplemented!("unsupported list data type: {:?}", $data_type),
                }
            }};
            (@make: $arrowty:ident) => {{
                type TypeBuilder = paste! {[< $arrowty Builder >]};
                Box::new(ListBuilder::with_capacity(
                    TypeBuilder::new(),
                    batch_size,
                ))
            }};
        }

    match dt {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            ConfiguredDecimal128Builder::with_capacity(0, *precision, *scale),
        ),
        DataType::Decimal256(precision, scale) => Box::new(
            ConfiguredDecimal256Builder::with_capacity(0, *precision, *scale),
        ),
        DataType::Dictionary(key_type, value_type) => {
            make_dictionary_builder!(key_type, value_type)
        }
        DataType::List(fields) => {
            make_list_builder!(fields.data_type().clone())
        }
        dt => make_builder(dt, batch_size),
    }
}

pub struct NullBuilder {
    len: usize,
}
impl NullBuilder {
    pub fn new() -> Self {
        Self { len: 0 }
    }

    pub fn append(&mut self) {
        self.len += 1;
    }

    pub fn extend(&mut self, len: usize) {
        self.len += len;
    }
}

impl ArrayBuilder for NullBuilder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish(&mut self) -> ArrayRef {
        self.finish_cloned()
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(NullArray::new(self.len))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub struct ConfiguredDecimalBuilder<T: DecimalType> {
    inner: PrimitiveBuilder<T>,
    precision: u8,
    scale: i8,
}

#[allow(unused)]
impl<T: DecimalType> ConfiguredDecimalBuilder<T> {
    pub fn with_capacity(capacity: usize, precision: u8, scale: i8) -> Self {
        Self {
            inner: PrimitiveBuilder::with_capacity(capacity),
            precision,
            scale,
        }
    }

    pub fn append_value(&mut self, v: T::Native) {
        self.inner.append_value(v)
    }

    pub fn append_option(&mut self, v: Option<T::Native>) {
        self.inner.append_option(v)
    }

    pub fn append_null(&mut self) {
        self.inner.append_null()
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}

impl<T: DecimalType> ArrayBuilder for ConfiguredDecimalBuilder<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(
            self.inner
                .finish()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap(),
        )
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(
            self.inner
                .finish_cloned()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
pub type ConfiguredDecimal128Builder = ConfiguredDecimalBuilder<Decimal128Type>;
pub type ConfiguredDecimal256Builder = ConfiguredDecimalBuilder<Decimal256Type>;

#[test]
fn test_struct_array_from_vec() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
    ]));
    let ints: ArrayRef =
        Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

    let arr =
        StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
            .unwrap();

    eprintln!("ans is: {:#?}",arr.is_valid(1))
}
