"""Download csv files from tlc website.

Download csv files one by one.
Store the information which file was downloaded in the db.

"""
import time
import datetime
import requests
from contextlib import closing
import csv
from loguru import logger
import asyncio

from data_types import TaxiTripFileDownloads
from base import DoesNotExist
from config import Config

config = Config()

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")

# Example URL: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2010-03.csv
TLC_BASE_URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/"


def create_tlc_url_from_data(month, year, type_="yellow"):
    filename = type_ + "_tripdata_" + str(year) + "-" + str(month).zfill(2) + ".csv"
    return TLC_BASE_URL + filename


def handle_data_rows_from_url(src_url, handler, *handler_args):
    """Stream data from url and pass each row to a handler.

    src_url: Source url from where to read the file.
        NOTE: A CSV file is expected.
    
    handler: A callable to process a single line.

    handler_args: All arguments that are passed to the handler in addition
        to the line.

    The destination file is not loaded into memory but rather streamed,
    line by line. Every line is then passed to a handler which processes the line
    in the background.

    If the filename is stored in the DB, do not attempt to read the file again.
    """
    logger.info(f"Start streaming from: {src_url}")

    url_copy = src_url
    filename = url_copy.split("/")[-1]
    try:
        loaded_file = TaxiTripFileDownloads.get(filename=filename)
        if loaded_file.is_processed:
            logger.warning(
                f"Already retrieved this file on {loaded_file.datetime_processed}."
            )
            return None
    except DoesNotExist:
        logger.info(f"File not found in DB, continue...")

    event_loop = asyncio.get_event_loop()

    with closing(requests.get(src_url, stream=True)) as r:
        r.encoding = "utf-8"
        reader = csv.reader(
            r.iter_lines(decode_unicode=True), delimiter=",", quotechar='"'
        )
        for row in reader:
            _handler_args = row, *handler_args
            call_in_background(handler, *_handler_args, loop=event_loop)

    TaxiTripFileDownloads.create(
        id=None,
        filename=filename,
        datetime_processed=datetime.datetime.utcnow(),
        is_processed=True,
    )
    logger.info(f"Finished streaming from {src_url}")
    return None


def handle_data_rows_from_file(filename, handler, *handler_args):
    """Read data from file and pass each row to a handler.

    filename: Source filename from where to read the data.
        NOTE: A CSV file is expected.
    
    handler: A callable to process a single line.

    handler_args: All arguments that are passed to the handler in addition
        to the line.

    Every line is passed to a handler which processes the line
    in the background.

    If the filename is stored in the DB, do not attempt to read the file again.
    """
    logger.info(f"Start reading from: {filename}")

    try:
        loaded_file = TaxiTripFileDownloads.get(filename=filename)
        if loaded_file.is_processed:
            logger.warning(
                f"Already retrieved this file on {loaded_file.datetime_processed}."
            )
            return None
    except DoesNotExist:
        logger.info(f"File not found in DB, continue...")

    event_loop = asyncio.get_event_loop()

    with open(config.data_dir / filename) as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        for row in reader:
            _handler_args = row, *handler_args
            call_in_background(handler, *_handler_args, loop=event_loop)

    # TODO: This is not executed. WHY?
    time.sleep(2)
    ttfd = TaxiTripFileDownloads.create(
        id=None,
        filename=filename,
        datetime_processed=datetime.datetime.utcnow(),
        is_processed=True,
    )
    logger.info(f"Finished reading from {filename}")
    return None


def call_in_background(target, *args, loop=None, executor=None):
    """Schedules and starts target callable as a background task

    If not given, *loop* defaults to the current thread's event loop
    If not given, *executor* defaults to the loop's default executor

    Returns the scheduled task.
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    if callable(target):
        return loop.run_in_executor(executor, target, *args)
    raise TypeError("target must be a callable, " "not {!r}".format(type(target)))
