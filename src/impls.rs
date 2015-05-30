use std::io::prelude::*;
use std::error;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use postgres;
use postgres::error::Error;
use postgres::types::{Type, Kind, ToSql, FromSql, Oid, IsNull, SessionInfo};

use {Array, Dimension};

impl<T> FromSql for Array<Option<T>> where T: FromSql {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, info: &SessionInfo)
                         -> postgres::Result<Array<Option<T>>> {
        let element_type = match ty.kind() {
            &Kind::Array(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        let ndim = try!(raw.read_u32::<BigEndian>()) as usize;
        let _has_null = try!(raw.read_i32::<BigEndian>()) == 1;
        let _element_type: Oid = try!(raw.read_u32::<BigEndian>());

        let mut dim_info = Vec::with_capacity(ndim);
        for _ in (0..ndim) {
            dim_info.push(Dimension {
                len: try!(raw.read_u32::<BigEndian>()) as usize,
                lower_bound: try!(raw.read_i32::<BigEndian>()) as isize,
            });
        }
        let nele = if dim_info.len() == 0 {
            0
        } else {
            dim_info.iter().fold(1, |product, info| product * info.len)
        };

        let mut elements = Vec::with_capacity(nele);
        for _ in (0..nele) {
            let len = try!(raw.read_i32::<BigEndian>());
            if len < 0 {
                elements.push(None);
            } else {
                let mut limit = raw.take(len as u64);
                elements.push(Some(try!(FromSql::from_sql(&element_type, &mut limit, info))));
                if limit.limit() != 0 {
                    let err: Box<error::Error+Sync+Send> =
                        "from_sql call did not consume all data".into();
                    return Err(Error::Conversion(err));
                }
            }
        }

        Ok(Array::from_parts(elements, dim_info))
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref ty) => <T as FromSql>::accepts(ty),
            _ => false
        }
    }
}

impl<T> ToSql for Array<T> where T: ToSql {
    fn to_sql<W: ?Sized+Write>(&self, ty: &Type, mut w: &mut W, info: &SessionInfo)
                               -> postgres::Result<IsNull> {
        let element_type = match ty.kind() {
            &Kind::Array(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        try!(w.write_u32::<BigEndian>(self.dimensions().len() as u32));
        try!(w.write_i32::<BigEndian>(1));
        try!(w.write_u32::<BigEndian>(element_type.oid()));

        for info in self.dimensions() {
            try!(w.write_u32::<BigEndian>(info.len as u32));
            try!(w.write_i32::<BigEndian>(info.lower_bound as i32));
        }

        let mut inner_buf = vec![];
        for v in self {
            match try!(v.to_sql(element_type, &mut inner_buf, info)) {
                IsNull::Yes => try!(w.write_i32::<BigEndian>(-1)),
                IsNull::No => {
                    try!(w.write_i32::<BigEndian>(inner_buf.len() as i32));
                    try!(w.write_all(&inner_buf));
                }
            }
            inner_buf.clear();
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

    use postgres::{Connection, SslMode};
    use postgres::types::{FromSql, ToSql};
    use Array;

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

            let tests = &[(Some(Array::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                          format!("'{{{},{},NULL}}'", $s1, $s2)),
                         (None, "NULL".to_string())];
            test_type(&format!("{}[]", $name), tests);
            let mut a = Array::from_vec(vec!(Some($v1), Some($v2)), 0);
            a.wrap(-1);
            a.push(Array::from_vec(vec!(None, Some($v3)), 0));
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
        stmt.query(&[]).unwrap().iter().next().unwrap().get::<_, Array<Option<i32>>>(0);
    }
}
