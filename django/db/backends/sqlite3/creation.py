import os
import sys

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.creation import BaseDatabaseCreation
from django.utils.crypto import get_random_string
from django.utils.six.moves import input


class DatabaseCreation(BaseDatabaseCreation):
    # SQLite doesn't actually support most of these types, but it "does the right
    # thing" given more verbose field definitions, so leave them as is so that
    # schema inspection is more useful.
    data_types = {
        'AutoField': 'integer',
        'BinaryField': 'BLOB',
        'BooleanField': 'bool',
        'CharField': 'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'datetime',
        'DecimalField': 'decimal',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'real',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'char(15)',
        'GenericIPAddressField': 'char(39)',
        'NullBooleanField': 'bool',
        'OneToOneField': 'integer',
        'PositiveIntegerField': 'integer unsigned',
        'PositiveSmallIntegerField': 'smallint unsigned',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField': 'text',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }
    data_types_suffix = {
        'AutoField': 'AUTOINCREMENT',
    }

    def sql_for_pending_references(self, model, style, pending_references):
        "SQLite3 doesn't support constraints"
        return []

    def sql_remove_table_constraints(self, model, references_to_delete, style):
        "SQLite3 doesn't support constraints"
        return []

    def _get_test_db_name(self):
        test_database_name = self.connection.settings_dict['TEST']['NAME']
        if test_database_name and test_database_name != ':memory:':
            if 'mode=memory' in test_database_name:
                raise ImproperlyConfigured(
                    "Using `mode=memory` parameter in the database name is not allowed, "
                    "use `:memory:` instead.")
            return test_database_name
        # Ticket 12118 - sqlite3 with version >=3.7.13 can share in-memory database
        # between threads. Built-in sqlite module of python2 comes without
        # ability to specify in-memory database as URI, but since python3.4 can do this,
        # so need to return URI to prevent fails in tests.
        if sys.version_info[:2] == (3, 4):
            return 'file:memorydb%s?mode=memory&cache=shared' % get_random_string()
        return ':memory:'

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        test_database_name = self._get_test_db_name()
        if keepdb:
            return test_database_name
        if test_database_name != ':memory:' and 'mode=memory' not in test_database_name:
            # Erase the old test database
            if verbosity >= 1:
                print("Destroying old test database '%s'..." % self.connection.alias)
            if os.access(test_database_name, os.F_OK):
                if not autoclobber:
                    confirm = input(
                        "Type 'yes' if you would like to try deleting the test "
                        "database '%s', or 'no' to cancel: " % test_database_name
                    )
                if autoclobber or confirm == 'yes':
                    try:
                        os.remove(test_database_name)
                    except Exception as e:
                        sys.stderr.write("Got an error deleting the old test database: %s\n" % e)
                        sys.exit(2)
                else:
                    print("Tests cancelled.")
                    sys.exit(1)
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        if test_database_name and (test_database_name != ":memory:" and 'mode=memory' not in test_database_name):
            # Remove the SQLite database file
            os.remove(test_database_name)

    def test_db_signature(self):
        """
        Returns a tuple that uniquely identifies a test database.

        This takes into account the special cases of ":memory:" and "" for
        SQLite since the databases will be distinct despite having the same
        TEST NAME. See http://www.sqlite.org/inmemorydb.html
        """
        test_dbname = self._get_test_db_name()
        sig = [self.connection.settings_dict['NAME']]
        if test_dbname == ':memory:' or 'mode=memory' in test_dbname:
            sig.append(self.connection.alias)
        return tuple(sig)
