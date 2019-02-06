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
    """Ensure that the the connection is always closed.
    
    This is not thread-safe, requires investigation.

    """
    connection = psycopg2.connect(**config.db_data)
    yield connection
    connection.close()


# def wait_for_release(func):
#     def wrapped_func(*args, **kwargs):
#         await config.async_lock.acquire()
#         try:
#             result = func(*args, **kwargs)
#         finally:
#             config.async_lock.release()

#         return result
#     return wrapped_func


@contextmanager
def open_cursor(_connection=None):
    """Ensure that the cursor is always closed.

    """
    cursor = _connection.cursor()
    yield cursor
    cursor.close()


def run_from_script(filename, query_data=None, commit=False):
    """Run SQL query from filename."""
    logger.info(f"Run {filename} on DB.")

    with open(filename, "r") as sql_file:
        sql = sql_file.read()

    sql_commands = sql.split(";")[:-1]
    with open_connection() as conn:
        with open_cursor(conn) as cursor:
            for sql_command in sql_commands:
                sql_command += ";"
                if query_data:
                    cursor.execute(sql_command, query_data)
                else:
                    cursor.execute(sql_command)

                if commit:
                    logger.info(f"Committing: {sql_command}")
                    conn.commit()
