from sqlalchemy.dialects.cassandra import base, cassandracql

# default dialect
base.dialect = cassandracql.dialect

from sqlalchemy.dialects.cassandra.base import UUID, dialect

__all__ = ( 'UUID', 'dialect' )

