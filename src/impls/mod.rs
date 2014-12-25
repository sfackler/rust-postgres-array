use std::io::ByRefReader;
use std::io::util::LimitReader;
use std::iter::MultiplicativeIterator;

use time::Timespec;
use serialize::json::Json;
use postgres::{mod, Error};
use postgres::types::{RawFromSql, ToSql, RawToSql, Type, Oid};

use {Array, ArrayBase, DimensionInfo};

macro_rules! check_types {
    ($($expected:pat)|+, $actual:ident) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(::postgres::Error::WrongType(actual.clone()))
        }
    )
}

macro_rules! from_sql_impl {
    ($($oid:pat)|+, $t:ty) => {
        impl ::postgres::FromSql for Option<::ArrayBase<Option<$t>>> {
            fn from_sql(ty: &::postgres::Type, raw: Option<&[u8]>) -> ::postgres::Result<Self> {
                check_types!($($oid)|+, ty);

                match raw {
                    Some(mut raw) => ::postgres::types::RawFromSql::raw_from_sql(&mut raw).map(Some),
                    None => Ok(None),
                }
            }
        }

        impl ::postgres::FromSql for ::ArrayBase<Option<$t>> {
            fn from_sql(ty: &::postgres::Type, raw: Option<&[u8]>) -> ::postgres::Result<Self> {
                let v: ::postgres::Result<Option<Self>> = ::postgres::FromSql::from_sql(ty, raw);
                match v {
                    Ok(None) => Err(::postgres::Error::WasNull),
                    Ok(Some(v)) => Ok(v),
                    Err(err) => Err(err),
                }
            }
        }
    }
}

macro_rules! to_sql_impl {
    ($($oid:pat)|+, $t:ty) => {
        impl ::postgres::ToSql for ::ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty);
                Ok(Some(::impls::raw_to_array(self, ty)))
            }
        }

        impl ::postgres::ToSql for Option<::ArrayBase<Option<$t>>> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty);
                match *self {
                    Some(ref arr) => arr.to_sql(ty),
                    None => Ok(None)
                }
            }
        }
    }
}


#[cfg(feature = "uuid")]
mod uuid;

impl<T> RawFromSql for ArrayBase<Option<T>> where T: RawFromSql {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> postgres::Result<ArrayBase<Option<T>>> {
        let ndim = try!(raw.read_be_u32()) as uint;
        let _has_null = try!(raw.read_be_i32()) == 1;
        let _element_type: Oid = try!(raw.read_be_u32());

        let mut dim_info = Vec::with_capacity(ndim);
        for _ in range(0, ndim) {
            dim_info.push(DimensionInfo {
                len: try!(raw.read_be_u32()) as uint,
                lower_bound: try!(raw.read_be_i32()) as int,
            });
        }
        let nele = dim_info.iter().map(|info| info.len as uint).product();

        let mut elements = Vec::with_capacity(nele);
        for _ in range(0, nele) {
            let len = try!(raw.read_be_i32());
            if len < 0 {
                elements.push(None);
            } else {
                let mut limit = LimitReader::new(raw.by_ref(), len as uint);
                elements.push(Some(try!(RawFromSql::raw_from_sql(&mut limit))));
                if limit.limit() != 0 {
                    return Err(Error::BadData);
                }
            }
        }

        Ok(ArrayBase::from_raw(elements, dim_info))
    }
}

from_sql_impl!(Type::BoolArray, bool);
from_sql_impl!(Type::ByteAArray, Vec<u8>);
from_sql_impl!(Type::CharArray, i8);
from_sql_impl!(Type::Int2Array, i16);
from_sql_impl!(Type::Int4Array, i32);
from_sql_impl!(Type::TextArray | Type::CharNArray | Type::VarcharArray | Type::NameArray, String);
from_sql_impl!(Type::Int8Array, i64);
from_sql_impl!(Type::JsonArray, Json);
from_sql_impl!(Type::Float4Array, f32);
from_sql_impl!(Type::Float8Array, f64);
from_sql_impl!(Type::TimestampArray | Type::TimestampTZArray, Timespec);

fn raw_to_array<T>(array: &ArrayBase<Option<T>>, ty: &Type) -> Vec<u8> where T: RawToSql {
    let mut buf = vec![];

    let _ = buf.write_be_i32(array.dimension_info().len() as i32);
    let _ = buf.write_be_i32(1);
    let _ = buf.write_be_u32(ty.member_type().to_oid());

    for info in array.dimension_info().iter() {
        let _ = buf.write_be_i32(info.len as i32);
        let _ = buf.write_be_i32(info.lower_bound as i32);
    }

    for v in array.values() {
        match *v {
            Some(ref val) => {
                let mut inner_buf = vec![];
                let _ = val.raw_to_sql(&mut inner_buf);
                let _ = buf.write_be_i32(inner_buf.len() as i32);
                let _ = buf.write(&*inner_buf);
            }
            None => {
                let _ = buf.write_be_i32(-1);
            }
        }
    }

    buf
}

to_sql_impl!(Type::BoolArray, bool);
to_sql_impl!(Type::ByteAArray, Vec<u8>);
to_sql_impl!(Type::CharArray, i8);
to_sql_impl!(Type::Int2Array, i16);
to_sql_impl!(Type::Int4Array, i32);
to_sql_impl!(Type::Int8Array, i64);
to_sql_impl!(Type::TextArray | Type::CharNArray | Type::VarcharArray | Type::NameArray, String);
to_sql_impl!(Type::Float4Array, f32);
to_sql_impl!(Type::Float8Array, f64);
to_sql_impl!(Type::JsonArray, Json);
to_sql_impl!(Type::TimestampArray | Type::TimestampTZArray, Timespec);

#[cfg(test)]
mod test {
    use std::fmt;

    use postgres::{Connection, SslMode, FromSql, ToSql};

    fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Show>(sql_type: &str, checks: &[(T, S)]) {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        for &(ref val, ref repr) in checks.iter() {
            let stmt = conn.prepare(format!("SELECT {}::{}", *repr, sql_type)[]).unwrap();
            let result = stmt.query(&[]).unwrap().next().unwrap().get(0u);
            assert!(val == &result);

            let stmt = conn.prepare(format!("SELECT $1::{}", sql_type)[]).unwrap();
            let result = stmt.query(&[val]).unwrap().next().unwrap().get(0u);
            assert!(val == &result);
        }
    }

    macro_rules! test_array_params {
        ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({
            use ArrayBase;

            let tests = &[(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                          format!("'{{{},{},NULL}}'", $s1, $s2).into_string()),
                         (None, "NULL".to_string())];
            test_type(format!("{}[]", $name)[], tests);
            let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
            a.wrap(-1);
            a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
            let tests = &[(Some(a), format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'",
                                           $s1, $s2, $s3).into_string())];
            test_type(format!("{}[][]", $name)[], tests);
        })
    }

    #[test]
    fn test_boolarray_params() {
        test_array_params!("BOOL", false, "f", true, "t", true, "t");
    }

    #[test]
    fn test_byteaarray_params() {
        test_array_params!("BYTEA", vec!(0u8, 1), r#""\\x0001""#, vec!(254u8, 255u8),
                           r#""\\xfeff""#, vec!(10u8, 11u8), r#""\\x0a0b""#);
    }

    #[test]
    fn test_chararray_params() {
        test_array_params!("\"char\"", 'a' as i8, "a", 'z' as i8, "z",
                           '0' as i8, "0");
    }

    #[test]
    fn test_namearray_params() {
        test_array_params!("NAME", "hello".to_string(), "hello", "world".to_string(),
                           "world", "!".to_string(), "!");
    }

    #[test]
    fn test_int2array_params() {
        test_array_params!("INT2", 0i16, "0", 1i16, "1", 2i16, "2");
    }

    #[test]
    fn test_int4array_params() {
        test_array_params!("INT4", 0i32, "0", 1i32, "1", 2i32, "2");
    }

    #[test]
    fn test_textarray_params() {
        test_array_params!("TEXT", "hello".to_string(), "hello", "world".to_string(),
                           "world", "!".to_string(), "!");
    }

    #[test]
    fn test_charnarray_params() {
        test_array_params!("CHAR(5)", "hello".to_string(), "hello",
                           "world".to_string(), "world", "!    ".to_string(), "!");
    }

    #[test]
    fn test_varchararray_params() {
        test_array_params!("VARCHAR", "hello".to_string(), "hello",
                           "world".to_string(), "world", "!".to_string(), "!");
    }

    #[test]
    fn test_int8array_params() {
        test_array_params!("INT8", 0i64, "0", 1i64, "1", 2i64, "2");
    }

    #[test]
    fn test_float4array_params() {
        test_array_params!("FLOAT4", 0f32, "0", 1.5f32, "1.5", 0.009f32, ".009");
    }

    #[test]
    fn test_float8array_params() {
        test_array_params!("FLOAT8", 0f64, "0", 1.5f64, "1.5", 0.009f64, ".009");
    }
}
