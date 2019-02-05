"""Connect to Postgres DB and run commands.

"""
import psycopg2
from loguru import logger
from contextlib import contextmanager

from config import Config

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
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
    logger.info(f"Run {filename} on DB.")

    with open(filename, "r") as sql_file:
        sql = sql_file.read()

    sql_commands = sql.split(";")
    with open_connection() as conn:
        with open_cursor(conn) as cursor:
            for sql_command in sql_commands:
                if query_data:
                    cursor.execute(sql_command, query_data)
                else:
                    cursor.execute(sql_command)

                if commit:
                    logger.info(f"Committing: {sql_command}")
                    conn.commit()
