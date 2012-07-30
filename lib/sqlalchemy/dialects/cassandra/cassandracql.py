from sqlalchemy.dialects.cassandra.base import CassandraDialect

class CassandraDialect_cql(CassandraDialect):

    driver = 'cql'

    def __init__(self, **kwargs):
        CassandraDialect.__init__(self, **kwargs)

    @classmethod
    def dbapi(cls):
        import cql
        return cql

dialect = CassandraDialect_cql

