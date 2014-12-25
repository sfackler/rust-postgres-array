extern crate uuid;

use postgres::Type;
use self::uuid::Uuid;

from_sql_impl!(Type::Uuid, Uuid);
to_sql_impl!(Type::Uuid, Uuid);
