"""CLI to run all aspects of the mcnulty pipeline


"""

import argparse
import sys
from loguru import logger

help_text = """
Project McNulty - Metis
Run the pipeline.

"""

epilog = """
Written as first draft by Moritz Eilfort.
"""


# reporting_logger = logging.getLogger("dobby")

parser = argparse.ArgumentParser(
    prog="mcnulty",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=help_text,
    epilog=epilog,
)

parser.add_argument(
    "--setup_db", "-s", help="Setup the database. Create necessary Tables."
)

# parser.add_argument('--list_channels', '-l',
#     action='store_true',
#     help='List all available channels.'
# )

# parser.add_argument('--clean', '-c',
#     help='Clean Data.'
# )

# parser.add_argument('--listen',
#     action='store_true',
#     help='Listen for messages.'
# )

# parser.add_argument('--seconds',
#     help='Listen for X seconds (default=30).'
# )


def main():
    args = parser.parse_args()

    # if args.list_channels:
    #     print(f"args.list_channels {args.list_channels}.")
    #     dobby.print_channels()
    #     sys.exit(0)

    # elif args.listen:
    #     print("Listen")
    #     if not args.seconds:
    #         dobby.listen_for(180)
    #     else:
    #         # dobby.listen_for(int(args.seconds))
    #         dobby.listen_for(180)

    # elif args.send_message:
    #     print(f"args.send_message {args.send_message}.")
    #     dobby.send_message(msg=args.send_message, channel_name=args.channel_name)
    #     sys.exit(0)

    # else:
    #     sys.exit(1)


if __name__ == "__main__":
    main()
