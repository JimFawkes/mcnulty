from psycopg2 import ProgrammingError, IntegrityError
import datetime
from loguru import logger

from db.connect import open_cursor, open_connection


_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")


class DataTypeSaveError(Exception):
    pass


class TypeValidationError(Exception):
    pass


class MultipleRowsError(Exception):
    pass


class DoesNotExist(Exception):
    pass


class BaseDataClass:
    def _create_insert_query(self):

        column_names = ""
        row_values = ""
        for column_name, row_value in self.__dict__.items():

            if column_name.startswith("_"):
                continue

            if column_name == "id" and row_value is None:
                # If id is None, leave it to the db to deal with incrementing the pk.
                continue

            column_names += str(column_name) + ", "
            row_values += "'" + str(row_value) + "'" + ", "

        columns = "(" + column_names[:-2] + ")"
        values = "(" + row_values[:-2] + ")"

        query = f"INSERT INTO {self._table_name} {columns} VALUES {values};"

        return query

    @classmethod
    def _create_select_query(cls, **kwargs):

        key_value_pairs = ""
        for key, value in kwargs.items():
            key_value_pairs += f"{key} = '{value}' AND "

        key_value_pairs = key_value_pairs[:-5]

        query = f"SELECT * FROM {cls._table_name} WHERE {key_value_pairs};"

        return query

    def save(self, commit=True):
        """Store conent to database.
        This should be thread safe  by using asyncio's Lock in open_cursor.


        """
        self.validate()
        logger.debug(f"Save: {self}")
        query = self._create_insert_query()

        with open_connection() as conn:
            with open_cursor(conn) as cursor:
                try:
                    cursor.execute(query)
                    if commit:
                        conn.commit()
                except ProgrammingError as e:
                    logger.error(e)
                    raise DataTypeSaveError
                except IntegrityError as e:
                    logger.warning(f"Could not save: {self}")
                    logger.error(e)

        self.id = self.get(**self.__dict__)
        return self

    def clean(self):
        logger.debug(f"Cleaning: {self}")

    def validate(self):
        annotations = self.__annotations__
        keys_ = annotations.keys()
        fields = self.__dict__
        for key in keys_:
            if not isinstance(fields[key], annotations[key]):
                if key == "id" and fields[key] is None:
                    # Pass None to id and allow the DB to increment it.
                    continue
                try:
                    self.__dict__[key] = annotations[key](fields[key])
                except (TypeError, ValueError) as e:
                    logger.error(
                        f"Encountered wrong type for {key}, got {type(fields[key])} but expected: {annotations[key]}."
                    )
                    logger.error(e)
                    raise TypeValidationError(
                        f"Encountered wrong type for {key}, got {type(fields[key])} but expected: {annotations[key]}."
                    )

    @classmethod
    def _prepare(cls, **kwargs):

        return kwargs

    @classmethod
    def create(cls, **kwargs):
        prepared_kwargs = cls._prepare(**kwargs)
        inst = cls(**prepared_kwargs)
        inst.clean()
        inst.save()
        return inst

    @classmethod
    def _get_rows(cls, **kwargs):
        logger.debug(f"{cls}._get_rows")
        query = cls._create_select_query(**kwargs)

        with open_connection() as conn:
            with open_cursor(conn) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        return rows

    @classmethod
    def all(cls, **kwargs):
        logger.debug(f"Get all: {cls}")
        rows = cls._get_rows(**kwargs)
        instances = []
        for row in rows:
            instances.append(cls(*row))

        return instances

    @classmethod
    def get(cls, **kwargs):
        logger.debug(f"Get: {cls}")
        rows = cls._get_rows(**kwargs)
        logger.debug(f"Rows: {rows}")

        if not rows:
            raise DoesNotExist(f"{cls}({kwargs}")

        if len(rows) > 1:
            raise MultipleRowsError(f"Got {len(rows)} entries in {cls}.get()")

        if isinstance(rows, list):
            row = rows[0]
        else:
            row = rows

        return cls(*row)
