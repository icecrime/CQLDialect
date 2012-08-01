import uuid

from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler, expression

class UUID(sqltypes.UserDefinedType):

    __visit_name__ = 'UUID'

    def python_type(self):
        return uuid.UUID

colspecs = {
    'uuid': UUID
}

# From my understanding, ischema_names is meant for schema reflection, which
# is not something that will be supported by the Cassandra dialect because of
# a lack of describe command.
ischema_names= {}

class CassandraCompiler(compiler.SQLCompiler):

    def visit_cast(self, cast, **kwargs):
        raise NotSupported("CAST is not supported by Cassandra")

class CassandraTypeCompiler(compiler.GenericTypeCompiler):

    def visit_ASCII(self, type_):
        return 'ascii'

    def visit_BIGINT(self, type_):
        return 'bigint'

    def visit_BLOB(self, type_):
        return 'blob'

    def visit_BOOLEAN(self, type_):
        return 'boolean'

    def visit_FLOAT(self, type_):
        return 'float'

    def visit_INTEGER(self, type_):
        return 'int'

    def visit_TEXT(self, type_):
        return 'text'

    def visit_TIMESTAMP(self, type_):
        return 'timestamp'

    def visit_UUID(self, type_):
        return 'uuid'

    def visit_VARCHAR(self, type_):
        return 'varchar'

    def visit_string(self, type_):
        return self.visit_ASCII(type_)

    def visit_unicode(self, type_):
        return self.visit_VARCHAR(type_)

class CassandraDDLCompiler(compiler.DDLCompiler):

    def visit_create_table(self, create):
        table = create.element
        preparer = self.dialect.identifier_preparer

        # Cassandra _must_ have a PRIMARY KEY.
        text = 'CREATE TABLE %s (' % preparer.format_table(table)

        separator = '\n  '
        for column in table.columns:
            text += '%s%s %s' % (separator, preparer.format_column(column),
                                 self.dialect.type_compiler.process(column.type))

            if column.primary_key:
                text += ' PRIMARY KEY'
            separator = ',\n  '

        text += '\n) %s\n\n' % self.post_create_table(table)
        print text
        return text

class CassandraDialect(default.DefaultDialect):

    name = 'cassandra'
    driver = 'cql'

    ddl_compiler = CassandraDDLCompiler
    statement_compiler = CassandraCompiler
    type_compiler = CassandraTypeCompiler

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
            cparams['keyspace'] = cparams.pop('database')
        return super(CassandraDialect, self).connect(*cargs, **cparams)

    def has_table(self, connection, table_name, schema=None):
        # For some reason cqlsh has a DESCRIBE command which CQL does not. We
        # just attempt to select the table and test for an exception.
        cursor = connection.connection.cursor()

        try:
            # We assume the table exists if the SELECT is successful.
            cursor.execute('SELECT FIRST 1 * from %s' % table_name)
            row = cursor.fetchone()
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

