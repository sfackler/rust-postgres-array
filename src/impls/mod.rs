use std::io::ByRefReader;
use std::io::util::LimitReader;
use std::iter::MultiplicativeIterator;

use time::Timespec;
use serialize::json::Json;
use postgres::{self, Error};
use postgres::types::{RawFromSql, ToSql, RawToSql, Type, Oid};

use {Array, ArrayBase, DimensionInfo};

macro_rules! check_types {
    ($actual:ident, $($expected:pat),+) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(::postgres::Error::WrongType(actual.clone()))
        }
    )
}

macro_rules! from_sql_impl {
    ($t:ty, $($oid:pat),+) => {
        impl ::postgres::FromSql for Option<::ArrayBase<Option<$t>>> {
            fn from_sql(ty: &::postgres::Type, raw: Option<&[u8]>) -> ::postgres::Result<Self> {
                check_types!(ty, $($oid),+);

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
    ($t:ty, $($oid:pat),+) => {
        impl ::postgres::ToSql for ::ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!(ty, $($oid),+);
                Ok(Some(::impls::raw_to_array(self, ty)))
            }
        }

        impl ::postgres::ToSql for Option<::ArrayBase<Option<$t>>> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!(ty, $($oid),+);
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
        let ndim = try!(raw.read_be_u32()) as usize;
        let _has_null = try!(raw.read_be_i32()) == 1;
        let _element_type: Oid = try!(raw.read_be_u32());

        let mut dim_info = Vec::with_capacity(ndim);
        for _ in range(0, ndim) {
            dim_info.push(DimensionInfo {
                len: try!(raw.read_be_u32()) as usize,
                lower_bound: try!(raw.read_be_i32()) as isize,
            });
        }
        let nele = if dim_info.len() == 0 {
            0
        } else {
            dim_info.iter().map(|info| info.len as usize).product()
        };

        let mut elements = Vec::with_capacity(nele);
        for _ in range(0, nele) {
            let len = try!(raw.read_be_i32());
            if len < 0 {
                elements.push(None);
            } else {
                let mut limit = LimitReader::new(raw.by_ref(), len as usize);
                elements.push(Some(try!(RawFromSql::raw_from_sql(&mut limit))));
                if limit.limit() != 0 {
                    return Err(Error::BadData);
                }
            }
        }

        Ok(ArrayBase::from_raw(elements, dim_info))
    }
}

from_sql_impl!(bool, Type::BoolArray);
from_sql_impl!(Vec<u8>, Type::ByteAArray);
from_sql_impl!(i8, Type::CharArray);
from_sql_impl!(i16, Type::Int2Array);
from_sql_impl!(i32, Type::Int4Array);
from_sql_impl!(String, Type::TextArray, Type::CharNArray, Type::VarcharArray, Type::NameArray);
from_sql_impl!(i64, Type::Int8Array);
from_sql_impl!(Json, Type::JsonArray);
from_sql_impl!(f32, Type::Float4Array);
from_sql_impl!(f64, Type::Float8Array);
from_sql_impl!(Timespec, Type::TimestampArray, Type::TimestampTZArray);

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

to_sql_impl!(bool, Type::BoolArray);
to_sql_impl!(Vec<u8>, Type::ByteAArray);
to_sql_impl!(i8, Type::CharArray);
to_sql_impl!(i16, Type::Int2Array);
to_sql_impl!(i32, Type::Int4Array);
to_sql_impl!(i64, Type::Int8Array);
to_sql_impl!(String, Type::TextArray, Type::CharNArray, Type::VarcharArray, Type::NameArray);
to_sql_impl!(f32, Type::Float4Array);
to_sql_impl!(f64, Type::Float8Array);
to_sql_impl!(Json, Type::JsonArray);
to_sql_impl!(Timespec, Type::TimestampArray, Type::TimestampTZArray);

#[cfg(test)]
mod test {
    use std::fmt;

    use postgres::{Connection, SslMode, FromSql, ToSql};
    use ArrayBase;

    fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::String>(sql_type: &str, checks: &[(T, S)]) {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        for &(ref val, ref repr) in checks.iter() {
            let stmt = conn.prepare(&format!("SELECT {}::{}", *repr, sql_type)[]).unwrap();
            let result = stmt.query(&[]).unwrap().next().unwrap().get(0);
            assert!(val == &result);

            let stmt = conn.prepare(&format!("SELECT $1::{}", sql_type)[]).unwrap();
            let result = stmt.query(&[val]).unwrap().next().unwrap().get(0);
            assert!(val == &result);
        }
    }

    macro_rules! test_array_params {
        ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({

            let tests = &[(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                          format!("'{{{},{},NULL}}'", $s1, $s2)),
                         (None, "NULL".to_string())];
            test_type(&format!("{}[]", $name)[], tests);
            let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
            a.wrap(-1);
            a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
            let tests = &[(Some(a), format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'",
                                           $s1, $s2, $s3))];
            test_type(&format!("{}[][]", $name)[], tests);
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

    #[test]
    fn test_empty_array() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let stmt = conn.prepare("SELECT '{}'::INT4[]").unwrap();
        stmt.query(&[]).unwrap().next().unwrap().get::<_, ArrayBase<Option<i32>>>(0);
    }
}
