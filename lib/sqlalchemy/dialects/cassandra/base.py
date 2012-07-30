from sqlalchemy.engine import default
from sqlalchemy.sql import compiler, expression

class CassandraCompiler(compiler.SQLCompiler):

    def visit_cast(self, cast, **kwargs):
        raise NotSupported("Cassandra has no 'CAST'")

class CassandraDDLCompiler(compiler.DDLCompiler):

    def visit_create_table(self, create):
        table = create.element
        preparer = self.dialect.identifier_preparer

        # Cassandra _must_ have a PRIMARY KEY.
        text = 'CREATE TABLE ' + preparer.format_table(table)

class CassandraDialect(default.DefaultDialect):

    name = 'cassandra'
    driver = 'cql'

    ddl_compiler = CassandraDDLCompiler
    statement_compiler = CassandraCompiler

    supports_alter = False
    supports_cast = False
    supports_default_values = True
    supports_empty_insert = False
    supports_unicode_binds = True
    supports_unicode_statements = True

    def __init__(self, isolation_level=None, native_datetime=True, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    def _check_unicode_returns(self, connection):
        # DefaultDialect._check_unicode_returns cannot work with Cassandra: it
        # tries to run
        #   SELECT CAST('test plain returns' AS VARCHAR(60))
        #   SELECT CAST('test unicode returs' AS UNICODE(60))
        #
        # We know Cassandra has a full unicode support, so we just say yes.
        return True

    def connect(self, *cargs, **cparams):
        # Cassandra has keyspaces, not databases !
        if 'database' in cparams:
            cparams['keyspace'] = cparams['database']
            del cparams['database']
        return super(CassandraDialect, self).connect(*cargs, **cparams)

    def has_table(self, connection, table_name, schema=None):
        # For some reason cqlsh has a DESCRIBE command which CQL does not. We
        # just attempt to select the table and test for an exception.
        cursor = connection.connection.cursor()

        try:
            cursor.execute(
                str(expression.select(
                    [expression.column('*')],
                    from_obj=expression.table(table_name)
                ).compile(dialect=self))
            )
            row = cursor.fetchone()

            # We assume the table exists if the SELECT is successful.
            return True
        except self.dbapi.Error:
            # We assume that any error means that the table does not exist.
            return False

    def do_commit(self, connection):
        # No commit in Cassandra: silently ignore the request
        pass

    def do_rollback(self, connection):
        # No rollback in Cassandra: silently ignore the request
        pass

dialect = CassandraDialect

