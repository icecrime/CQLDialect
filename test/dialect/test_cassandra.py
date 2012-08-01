"""Cassandra specific tests."""

from sqlalchemy import *
from test.lib import *
from test.lib.testing import eq_

import sqlalchemy.dialects.cassandra as cassandra

class TestTypes(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'cassandra'

    supports_sequences = False

    def test_boolean(self):
        """Test that the boolean only treats 1 as True

        """

        meta = MetaData(testing.db)
        t = Table('test_table', meta,
                  Column('id', Integer, primary_key=True),
                  Column('key', cassandra.UUID()))

        try:
            meta.create_all()
        finally:
            meta.drop_all()

