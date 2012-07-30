from sqlalchemy.dialects.cassandra import base, cassandracql

# default dialect
base.dialect = cassandracql.dialect

