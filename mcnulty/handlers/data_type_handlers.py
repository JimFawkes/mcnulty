from loguru import logger

from config import Config
from data_types import Location
from base import DataTypeSaveError

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
    prepared_data = DataType.prepare(*data)
    dt = DataType(*prepared_data)
    dt.clean()
    try:
        dt.save(with_get=False)
    except DataTypeSaveError as e:
        logger.debug(f"TODO: Store {data} in file.")

