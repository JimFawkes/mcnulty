"""Retrieve weather info from the Dark Sky API.

Get observed hourly data from api.darksky.net and store it into the db.
"""
import requests
import datetime
import json
import pytz
from dataclasses import dataclass
from loguru import logger

from db.connect import open_connection, open_cursor
from config import Config

config = Config()

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")

DARK_SKY_API_URL = "https://api.darksky.net/forecast/{api_key}/40.77898,-73.96925,{epoch_time}?exclude=currently,minutely,alerts&units=si"

weather_insert_query = """
INSERT INTO hourly_weather (
    epoch_time,
    time,
    summary,
    icon,
    precipIntensity,
    precipProbability,
    precipType,
    temperature,
    apparentTemperature,
    dewPoint,
    humidity,
    pressure,
    windSpeed,
    windGust,
    windBearing,
    cloudCover,
    uvIndex,
    visibility)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


@dataclass(order=True)
class DarkSkyWeather:
    """
        Names are in CamelCase to allow dict unpacking from api response.
    """

    epoch_time: int
    time: datetime.datetime
    summary: str = None
    icon: str = None
    precipIntensity: float = None
    precipProbability: float = None
    precipType: str = None
    temperature: float = None
    apparentTemperature: float = None
    dewPoint: float = None
    humidity: float = None
    pressure: float = None
    windSpeed: float = None
    windGust: float = None
    windBearing: float = None
    cloudCover: float = None
    uvIndex: float = None
    visibility: float = None

    def get_values_as_string(self):
        return (
            self.epoch_time,
            self.time,
            self.summary,
            self.icon,
            self.precipIntensity,
            self.precipProbability,
            self.precipType,
            self.temperature,
            self.apparentTemperature,
            self.dewPoint,
            self.humidity,
            self.pressure,
            self.windSpeed,
            self.windGust,
            self.windBearing,
            self.cloudCover,
            self.uvIndex,
            self.visibility,
        )

    def save(self):
        logger.debug(f"Save: {self}")
        with open_connection() as conn:
            with open_cursor(conn) as cur:
                cur.execute(weather_insert_query, self.get_values_as_string())
                conn.commit()


def get_epoch_time_range(start=None, end=None):
    """Convert start/end time to epoch-time and retrun an hourly epoch range."""
    if start is None or end is None:
        start = datetime.datetime(2017, 1, 1, 0, 1, 1)
        end = datetime.datetime(2018, 12, 31, 23, 59, 30)

    logger.info(f"Get Epochs for {start} - {end}")
    epochs = []
    while start < end:
        epoch = int((start - datetime.datetime(1970, 1, 1)).total_seconds())
        epochs.append(epoch)
        start += datetime.timedelta(hours=1)

    return epochs


def get_epoch_range_for_year(year):
    start = datetime.datetime(year, 1, 1, 0, 1, 1)
    end = datetime.datetime(year, 12, 31, 23, 59, 30)
    epoch_range = get_epoch_time_range(start, end)
    return epoch_range


def convert_epoch_to_dt(epoch_time):
    """Make an epoch time readable by converting it to datetime."""
    return datetime.datetime(1970, 1, 1, 0, 0, 0) + datetime.timedelta(
        seconds=epoch_time
    )


def get_weather_info(epoch_time):
    """Get response from darksky api"""
    api_key = config.dark_sky_api_key
    response = requests.get(
        DARK_SKY_API_URL.format(api_key=api_key, epoch_time=epoch_time)
    )
    weather_info = json.loads(response.content)
    return weather_info


def insert_hourly_weather_to_db(weather_info):
    """Insert the weather info to the db."""
    logger.info(
        f"Inserting Weather data for {convert_epoch_to_dt(int(weather_info['daily']['data'][0]['time']))} to db."
    )
    hourly_weather = weather_info["hourly"]["data"]
    for hour in hourly_weather:
        hour["epoch_time"] = hour["time"]
        hour["time"] = convert_epoch_to_dt(hour["epoch_time"])
        if "ozone" in hour:
            hour.pop("ozone")
        logger.info(f"{hour}")
        weather = DarkSkyWeather(**hour)
        weather.save()


def insert_weather_into_db(year):
    """Insert an entire year into the db."""
    epoch_range = get_epoch_range_for_year(year)
    for epoch_time in epoch_range:
        weather_info = get_weather_info(epoch_time)
        insert_hourly_weather_to_db(weather_info)
