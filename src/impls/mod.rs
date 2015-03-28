use std::iter::MultiplicativeIterator;
use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use postgres::{self, Error, Type, Kind, ToSql, FromSql, Oid};
use postgres::types::{IsNull};

use {Array, ArrayBase, DimensionInfo};

impl<T> FromSql for ArrayBase<Option<T>> where T: FromSql {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> postgres::Result<ArrayBase<Option<T>>> {
        let element_type = match ty.kind() {
            &Kind::Array(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        let ndim = try!(raw.read_u32::<BigEndian>()) as usize;
        let _has_null = try!(raw.read_i32::<BigEndian>()) == 1;
        let _element_type: Oid = try!(raw.read_u32::<BigEndian>());

        let mut dim_info = Vec::with_capacity(ndim);
        for _ in (0..ndim) {
            dim_info.push(DimensionInfo {
                len: try!(raw.read_u32::<BigEndian>()) as usize,
                lower_bound: try!(raw.read_i32::<BigEndian>()) as isize,
            });
        }
        let nele = if dim_info.len() == 0 {
            0
        } else {
            dim_info.iter().map(|info| info.len as usize).product()
        };

        let mut elements = Vec::with_capacity(nele);
        for _ in (0..nele) {
            let len = try!(raw.read_i32::<BigEndian>());
            if len < 0 {
                elements.push(None);
            } else {
                let mut limit = raw.take(len as u64);
                elements.push(Some(try!(FromSql::from_sql(&element_type, &mut limit))));
                if limit.limit() != 0 {
                    return Err(Error::BadResponse);
                }
            }
        }

        Ok(ArrayBase::from_raw(elements, dim_info))
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref ty) => <T as FromSql>::accepts(ty),
            _ => false
        }
    }
}

impl<T> ToSql for ArrayBase<Option<T>> where T: ToSql {
    fn to_sql<W: ?Sized+Write>(&self, ty: &Type, mut w: &mut W) -> postgres::Result<IsNull> {
        let element_type = match ty.kind() {
            &Kind::Array(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        try!(w.write_u32::<BigEndian>(self.dimension_info().len() as u32));
        try!(w.write_i32::<BigEndian>(1));
        try!(w.write_u32::<BigEndian>(element_type.to_oid()));

        for info in self.dimension_info() {
            try!(w.write_u32::<BigEndian>(info.len as u32));
            try!(w.write_i32::<BigEndian>(info.lower_bound as i32));
        }

        for v in self.values() {
            match *v {
                Some(ref val) => {
                    let mut inner_buf = vec![];
                    try!(val.to_sql(element_type, &mut inner_buf));
                    try!(w.write_i32::<BigEndian>(inner_buf.len() as i32));
                    try!(w.write_all(&inner_buf));
                }
                None => {
                    try!(w.write_i32::<BigEndian>(-1));
                }
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref ty) => <T as ToSql>::accepts(ty),
            _ => false
        }
    }

    to_sql_checked!();
}

#[cfg(test)]
mod test {
    use std::fmt;

    use postgres::{Connection, SslMode, FromSql, ToSql};
    use ArrayBase;

    fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Display>(sql_type: &str, checks: &[(T, S)]) {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        for &(ref val, ref repr) in checks.iter() {
            let stmt = conn.prepare(&format!("SELECT {}::{}", *repr, sql_type)).unwrap();
            let result = stmt.query(&[]).unwrap().iter().next().unwrap().get(0);
            assert!(val == &result);

            let stmt = conn.prepare(&format!("SELECT $1::{}", sql_type)).unwrap();
            let result = stmt.query(&[val]).unwrap().iter().next().unwrap().get(0);
            assert!(val == &result);
        }
    }

    macro_rules! test_array_params {
        ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({

            let tests = &[(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                          format!("'{{{},{},NULL}}'", $s1, $s2)),
                         (None, "NULL".to_string())];
            test_type(&format!("{}[]", $name), tests);
            let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
            a.wrap(-1);
            a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
            let tests = &[(Some(a), format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'",
                                           $s1, $s2, $s3))];
            test_type(&format!("{}[][]", $name), tests);
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
        stmt.query(&[]).unwrap().iter().next().unwrap().get::<_, ArrayBase<Option<i32>>>(0);
    }
}
