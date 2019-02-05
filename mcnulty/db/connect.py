"""Connect to Postgres DB and run commands.

"""
import psycopg2
from loguru import logger
from contextlib import contextmanager

from mcnulty.config import Config

# logger.add()
config = Config()


@contextmanager
def open_connection():
    connection = psycopg2.connect(**config.db_data)
    yield connection
    connection.close()


@contextmanager
def open_cursor(_connection=None):
    """Ensure that the cursor and the connection are always closed.
    
    This should be thread-safe, requires tests.

    TODO: Test special cases and catch them.
    
    """
    #     async with config.async_lock:
    if not _connection:
        connection = psycopg2.connect(**config.db_data)
        cursor = connection.cursor()
    else:
        cursor = _connection.cursor()

    yield cursor

    cursor.close()
    if _connection:
        _connection.close()


def run_from_script(filename, query_data=None, commit=False):
    """Run SQL query from filename."""

    with open(filename, "r") as sql_file:
        sql = sql_file.read()

    sql_commands = sql.split(";")
    with open_cursor() as cursor:
        for sql_command in sql_commands:
            if query_data:
                cursor.execute(sql_command, query_data)
            else:
                cursor.execute(sql_command)

            if commit:
                cursor.commit()
