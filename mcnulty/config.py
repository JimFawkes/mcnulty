"""McNulty Configuration

Define all necessary configuration in this file.
"""
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()


def singleton(cls):
    """Ensure that only one instance of the class ever exists."""
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


@singleton
class Config:
    """Configurations for mcnulty"""

    def __init__(self):
        self.db_host = os.getenv("DB_HOST", default="127.0.0.1")
        self.db_port = int(os.getenv("DB_PORT", default="5432"))
        self.db_name = os.getenv("DB_NAME", default="postgres")
        self.db_user = os.getenv("DB_USER", default="postgres")
        self.db_password = os.getenv("DB_PASSWORD", default="")

    def __repr__(self):
        return "Config()"

    @property
    def db_data(self):
        return {
            "host": self.db_host,
            "port": self.db_port,
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }