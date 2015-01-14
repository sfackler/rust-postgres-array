extern crate uuid;

use postgres::Type;
use self::uuid::Uuid;

from_sql_impl!(Uuid, Type::Uuid);
to_sql_impl!(Uuid, Type::Uuid);
