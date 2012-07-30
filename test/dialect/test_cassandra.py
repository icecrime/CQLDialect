"""Cassandra specific tests."""

from sqlalchemy import *
from test.lib import *
from test.lib.testing import eq_

class TestTypes(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'cassandra'

    supports_sequences = False

    def test_boolean(self):
        """Test that the boolean only treats 1 as True

        """

        meta = MetaData(testing.db)
        t = Table('bool_table', meta, Column('id', Integer,
                  primary_key=True), Column('boo',
                  Boolean(create_constraint=False)))
        try:
            meta.create_all()
        finally:
            pass
            #meta.drop_all()

