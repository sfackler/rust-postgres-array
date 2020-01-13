use fallible_iterator::FallibleIterator;
use postgres_protocol;
use postgres_protocol::types;
use postgres_types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};
use std::error::Error;

use crate::{Array, Dimension};
use postgres_types::private::BytesMut;

impl<'de, T> FromSql<'de> for Array<T>
where
    T: FromSql<'de>,
{
    fn from_sql(ty: &Type, raw: &'de [u8]) -> Result<Array<T>, Box<dyn Error + Sync + Send>> {
        let element_type = match *ty.kind() {
            Kind::Array(ref ty) => ty,
            _ => unreachable!(),
        };

        let array = types::array_from_sql(raw)?;

        let dimensions = array
            .dimensions()
            .map(|d| {
                Ok(Dimension {
                    len: d.len,
                    lower_bound: d.lower_bound,
                })
            })
            .collect()?;

        let elements = array
            .values()
            .map(|v| FromSql::from_sql_nullable(element_type, v))
            .collect()?;

        Ok(Array::from_parts(elements, dimensions))
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref ty) => <T as FromSql>::accepts(ty),
            _ => false,
        }
    }
}

impl<T> ToSql for Array<T>
where
    T: ToSql,
{
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let element_type = match ty.kind() {
            &Kind::Array(ref ty) => ty,
            _ => unreachable!(),
        };

        let dimensions = self.dimensions().iter().map(|d| types::ArrayDimension {
            len: d.len,
            lower_bound: d.lower_bound,
        });
        let elements = self.iter();

        types::array_to_sql(
            dimensions,
            element_type.oid(),
            elements,
            |v, w| match v.to_sql(element_type, w) {
                Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                Err(e) => Err(e),
            },
            w,
        )?;

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref ty) => <T as ToSql>::accepts(ty),
            _ => false,
        }
    }

    to_sql_checked!();
}

#[cfg(test)]
mod test {
    use std::fmt;

    use crate::Array;
    use postgres::types::{FromSqlOwned, ToSql};
    use postgres::{Client, NoTls};

    fn test_type<T: PartialEq + FromSqlOwned + ToSql + Sync, S: fmt::Display>(
        sql_type: &str,
        checks: &[(T, S)],
    ) {
        let mut conn = Client::connect("postgres://postgres:password@localhost", NoTls).unwrap();
        for &(ref val, ref repr) in checks.iter() {
            let result = conn
                .query(&*format!("SELECT {}::{}", *repr, sql_type), &[])
                .unwrap()[0]
                .get(0);
            assert!(val == &result);

            let result = conn
                .query(&*format!("SELECT $1::{}", sql_type), &[val])
                .unwrap()[0]
                .get(0);
            assert!(val == &result);
        }
    }

    macro_rules! test_array_params {
        ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => {{
            let tests = &[
                (
                    Some(Array::from_vec(vec![Some($v1), Some($v2), None], 1)),
                    format!("'{{{},{},NULL}}'", $s1, $s2),
                ),
                (None, "NULL".to_string()),
            ];
            test_type(&format!("{}[]", $name), tests);
            let mut a = Array::from_vec(vec![Some($v1), Some($v2)], 0);
            a.wrap(-1);
            a.push(Array::from_vec(vec![None, Some($v3)], 0));
            let tests = &[(
                Some(a),
                format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'", $s1, $s2, $s3),
            )];
            test_type(&format!("{}[][]", $name), tests);
        }};
    }

    #[test]
    fn test_boolarray_params() {
        test_array_params!("BOOL", false, "f", true, "t", true, "t");
    }

    #[test]
    fn test_byteaarray_params() {
        test_array_params!(
            "BYTEA",
            vec![0u8, 1],
            r#""\\x0001""#,
            vec![254u8, 255u8],
            r#""\\xfeff""#,
            vec![10u8, 11u8],
            r#""\\x0a0b""#
        );
    }

    #[test]
    fn test_chararray_params() {
        test_array_params!("\"char\"", 'a' as i8, "a", 'z' as i8, "z", '0' as i8, "0");
    }

    #[test]
    fn test_namearray_params() {
        test_array_params!(
            "NAME",
            "hello".to_string(),
            "hello",
            "world".to_string(),
            "world",
            "!".to_string(),
            "!"
        );
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
        test_array_params!(
            "TEXT",
            "hello".to_string(),
            "hello",
            "world".to_string(),
            "world",
            "!".to_string(),
            "!"
        );
    }

    #[test]
    fn test_charnarray_params() {
        test_array_params!(
            "CHAR(5)",
            "hello".to_string(),
            "hello",
            "world".to_string(),
            "world",
            "!    ".to_string(),
            "!"
        );
    }

    #[test]
    fn test_varchararray_params() {
        test_array_params!(
            "VARCHAR",
            "hello".to_string(),
            "hello",
            "world".to_string(),
            "world",
            "!".to_string(),
            "!"
        );
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
        let mut conn = Client::connect("postgres://postgres@localhost", NoTls).unwrap();
        conn.query("SELECT '{}'::INT4[]", &[]).unwrap()[0].get::<_, Array<i32>>(0);
    }
}
