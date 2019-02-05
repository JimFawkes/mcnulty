"""CLI to run all aspects of the mcnulty pipeline


"""

import argparse
import sys
from loguru import logger

from mcnulty.data_mining import tlc
from mcnulty.data_types import Location
from mcnulty.handlers.data_type_handlers import clean_and_store

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

data_types = [
    "Location",
    "PaymentTypes",
    "RateCodes",
    "LocationCoordinates",
    "Weather",
    "TaxiTrip",
]

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
    choices=data_types,
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
        logger.debug("Create Tables")
        sys.exit(0)

    if args.read_from_file:
        if not args.type:
            parser.error("--read_from_file requires --type.")

        logger.debug("Read From File")
        sys.exit(0)

    if args.read_from_url:
        if not args.type:
            parser.error("--read_from_url requires --type.")

        logger.debug("Read From URL")
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
