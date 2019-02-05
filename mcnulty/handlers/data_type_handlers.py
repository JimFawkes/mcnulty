from loguru import logger

from mcnulty.config import Config
from mcnulty.data_types import Location
from mcnulty.base import DataTypeSaveError

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")


def location_handler(row, *args):
    logger.debug(f"location_handler, handling: {row}")
    location = Location(*row)
    location.clean()
    location.save()


@logger.catch
def clean_and_store(data, DataType, *args):
    logger.debug(f"handling: {DataType.__name__}({data})")
    dt = DataType(*data)
    dt.clean()
    try:
        dt.save()
    except DataTypeSaveError as e:
        logger.debug(f"TODO: Store {data} in file.")

