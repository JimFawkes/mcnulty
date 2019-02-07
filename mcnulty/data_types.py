"""Define all Classes representing psql tables.


"""

from dataclasses import dataclass
import datetime
from loguru import logger
import numpy as np

from base import BaseDataClass, DoesNotExist

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")


@dataclass(order=True)
class Location(BaseDataClass):
    id: int
    borough: str
    zone: str
    service_zone: str
    _table_name = "locations"
    _ignore_fields = []


@dataclass(order=True)
class PaymentTypes(BaseDataClass):
    id: int
    name: str
    _table_name = "payment_types"
    _ignore_fields = []


@dataclass(order=True)
class RateCodes(BaseDataClass):
    id: int
    name: str
    _table_name = "rate_codes"
    _ignore_fields = []


@dataclass(order=True)
class LocationCoordinates(BaseDataClass):
    id: int
    location_id: int
    latitude: float
    longitude: float
    _table_name = "location_coordinates"
    _ignore_fields = []


@dataclass(order=True)
class Weather(BaseDataClass):
    id: int
    station_id: str
    station_name: str
    date: datetime.date
    windspeed_avg: float
    percipitation: float
    snow: float
    snow_depth: float
    temperature_avg: float
    temperature_max: float
    temperature_min: float
    sunshine_duration: float
    _table_name = "weather"
    _ignore_fields = []

    @classmethod
    def prepare(cls, *args):
        exclude = (2, 3, 4, 7, 9, 11, 13, 15, 17, 19, 21)
        prepared = [None] + [arg for idx, arg in enumerate(args) if idx not in exclude]
        logger.debug(f"PREPED: {prepared}")
        return prepared

    def clean_date(self):
        cleaned_date = datetime.datetime.strptime(self.date, "%Y-%m-%d").date()
        self.date = cleaned_date
        return True

    def clean_temperature_avg(self):
        if not self.temperature_avg:
            cleaned_tmp_avg = (
                float(self.temperature_min) + float(self.temperature_max)
            ) / 2
            self.temperature_avg = cleaned_tmp_avg

    def clean_sunshine_duration(self):
        if not self.sunshine_duration or self.sunshine_duration == np.nan:
            self.sunshine_duration = None
            self._ignore_fields.append("sunshine_duration")

    def clean_windspeed_avg(self):
        if not self.windspeed_avg or self.windspeed_avg == np.nan:
            self.windspeed_avg = None
            self._ignore_fields.append("windspeed_avg")

    def clean(self):
        super().clean()
        self.clean_date()
        self.clean_temperature_avg()
        self.clean_sunshine_duration()


@dataclass(order=True)
class TaxiTrip(BaseDataClass):
    id: int
    vendor_id: int
    pickup_datetime: datetime.datetime
    dropoff_datetime: datetime.datetime
    passenger_count: int
    trip_distance: float
    rate_code_id: int
    store_and_fwd_flag: bool
    pickup_location_id: int
    dropoff_location_id: int
    payment_type_id: int
    fare_amount: float
    extra_surcharge: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float
    _table_name = "taxi_trip"
    _ignore_fields = []

    def clean_pickup_datetime(self):
        pu_dt = datetime.datetime.strptime(self.pickup_datetime, "%Y-%m-%d %H:%M:%S")
        self.pickup_datetime = pu_dt

    def clean_dropoff_datetime(self):
        do_dt = datetime.datetime.strptime(self.dropoff_datetime, "%Y-%m-%d %H:%M:%S")
        self.dropoff_datetime = do_dt

    def clean_trip_distance(self):
        try:
            self.trip_distance *= 1.609_344
        except TypeError:
            try:
                self.trip_distance = float(self.trip_distance) * 1.609_344
            except (ValueError, TypeError):
                self.trip_distance = None
                self._ignore_fields.append("trip_distance")
                logger.warning(f"Could not convert trip, setting to None.")

    def clean(self):
        super().clean()
        self.clean_pickup_datetime()
        self.clean_dropoff_datetime()

    @classmethod
    def prepare(cls, *args):
        return [None] + list(args)

    def create_features(self):
        if self.id is None:
            self.id = self.get_id()
            if self.id is None:
                raise DoesNotExist(
                    f"{self} has no id. Maybe this needs to be saved to the db first."
                )
        trip_duration = (self.dropoff_datetime - self.pickup_datetime).total_seconds()
        avg_speed = self.trip_distance / (trip_duration / 3600)
        taxi_trip_id = self.id

        # TODO: GET OWN ID

        TaxiTripFeatures.create(
            **{
                "id": None,
                "pu_date": self.pickup_datetime.date(),
                "do_date": self.dropoff_datetime.date(),
                "taxi_trip_id": taxi_trip_id,
                "trip_duration": trip_duration,
                "avg_speed": avg_speed,
            }
        )

    def save(self, commit=True, **kwargs):
        self = super().save(commit=commit, **kwargs)
        self.create_features()


@dataclass(order=True)
class TaxiTripFeatures(BaseDataClass):
    id: int
    pu_date: datetime.date
    do_date: datetime.date
    taxi_trip_id: int
    trip_duration: float
    avg_speed: float
    _table_name = "taxi_trip_features"
    _ignore_fields = []


@dataclass(order=True)
class TaxiTripFileDownloads(BaseDataClass):
    id: int
    filename: str
    datetime_processed: datetime.datetime
    is_processed: bool
    _table_name = "taxi_trip_file_downloads"
    _ignore_fields = []
