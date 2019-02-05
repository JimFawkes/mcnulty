"""CLI to run all aspects of the mcnulty pipeline


"""

import argparse
import sys
from loguru import logger

from data_mining import tlc
import data_types as dt
from handlers.data_type_handlers import clean_and_store
from db.connect import run_from_script
from data_mining.tlc import handle_data_rows_from_file
from config import Config

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")

config = Config()

help_text = """
Project McNulty - Metis

The pipeline consists fo the following steps:
    1.) Create Tables in Database
    2.) Insert data into DB
        a) Fetch data from src
        b) clean data
        c) validate data row
        d) insert into db
        e) create relationships
    3.) Post-Processing !!! NOT YET IMPLEMENTED
    4.) Modeling !!! NOT YET IMPLEMENTED
    5.) Validation !!! NOT YET IMPLEMENTED
    6.) Visualization !!! NOT YET IMPLEMENTED


"""

epilog = """

Written as first draft by Moritz Eilfort.

"""

data_types = {
    "Location": dt.Location,
    "PaymentTypes": dt.PaymentTypes,
    "RateCodes": dt.RateCodes,
    "LocationCoordinates": dt.LocationCoordinates,
    "Weather": dt.Weather,
    "TaxiTrip": dt.TaxiTrip,
}

parser = argparse.ArgumentParser(
    prog="mcnulty",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=help_text,
    epilog=epilog,
)

# parser.add_argument(
#     "--setup_db", "-s", help="Setup the database. Create necessary Tables."
# )

parser.add_argument(
    "--create_tables",
    action="store_true",
    help="Create the necessary tables in the database.",
)

parser.add_argument(
    "--read_from_file",
    help="Read data from local file, clean, validate and store the data.",
    type=str,
)

parser.add_argument(
    "--read_from_url",
    help="Read data from url, clean, validate and store in db.",
    type=str,
)

parser.add_argument(
    "--type",
    choices=data_types.keys(),
    help="Define what type of data is expected from the source.",
)

parser.add_argument("--model", "-m", action="store_true", help="Run modeling")

parser.add_argument("--validate", action="store_true", help="Run validation")

parser.add_argument(
    "--run_and_validate_all",
    action="store_true",
    help="Run and validate all defined models.",
)

parser.add_argument("--upload_logs", action="store_true", help="Upload logs to S3.")


def main():
    args = parser.parse_args()
    logger.debug(f"Starting Pipline")

    if args.create_tables:
        logger.debug(f"Create Tables ({args.create_tables}).")
        run_from_script(config.project_dir / "sql/create_tables.sql", commit=True)
        sys.exit(0)

    if args.read_from_file:
        if not args.type:
            parser.error("--read_from_file requires --type.")

        filename = args.read_from_file
        _type = args.type

        logger.debug(f"Read From File. filename: {filename}, type: {_type}")
        handle_data_rows_from_file(filename, clean_and_store, data_types[_type])

        sys.exit(0)

    if args.read_from_url:
        if not args.type:
            parser.error("--read_from_url requires --type.")

        url = args.read_from_url
        _type = args.type
        logger.debug(f"Read From URL: {url}, type: {_type}")
        sys.exit(0)

    if args.type:
        parser.error(
            "--type can only be set in combination with --read_from_url or --read_from_file"
        )

    if args.model:
        logger.warning(f"Modeling part is not yet implemented.")
        sys.exit(1)

    if args.validate:
        logger.warning(f"Validation part is not yet implemented.")
        sys.exit(1)

    if args.run_and_validate:
        logger.warning(f"Run and Validate part is not yet implemented.")
        sys.exit(1)

    if args.upload_logs:
        logger.warning(f"Upload logs part is not yet implemented.")
        sys.exit(1)


if __name__ == "__main__":
    main()
