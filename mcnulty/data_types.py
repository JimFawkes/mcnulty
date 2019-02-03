"""Define all Classes representing psql tables.


"""
from mcnulty.db.connect import open_cursor

from dataclasses import dataclass
import datetime


class BaseDataClass:
    def _create_insert_query(self):

        column_names = ""
        row_values = ""
        for column_name, row_value in self.__dict__.items():

            if column_name.startswith("_"):
                continue

            column_names += str(column_name) + ", "
            row_values += str(row_value) + ", "

        columns = "(" + column_names[:-2] + ")"
        values = "(" + row_values[:-1] + ")"

        query = f"INSERT INTO {self._table_name} {columns} VALUES {values};"

        return query

    def save(self, commit=True):
        """Store conent to database."""
        query = self._create_insert_query()

        with open_cursor() as cursor:
            cursor.execute(query)
            if commit:
                cursor.commit()


@dataclass(order=True)
class Location(BaseDataClass):
    id: int
    borough: str
    zone: str
    service_zone: str
    _table_name = "locations"


@dataclass(order=True)
class PaymentTypes(BaseDataClass):
    id: int
    name: str
    _table_name = "payment_types"


@dataclass(order=True)
class RateCodes(BaseDataClass):
    id: int
    name: str
    _table_name = "rate_codes"


@dataclass(order=True)
class LocationCoordinates(BaseDataClass):
    id: int
    location_id: int
    latitude: float
    longitude: float
    _table_name = "location_coordinates"


@dataclass(order=True)
class Weather(BaseDataClass):
    id: int
    station_id: str
    station_name: str
    date: datetime.date
    temperature_min: float
    temperature_avg: float
    temperature_max: float
    percipitation: float
    windspeed_avg: float
    snow: float
    snow_depth: float
    sunshine_duration: float
    _table_name = "weather"


@dataclass(order=True)
class TaxiTrip(BaseDataClass):
    id: int
    vendor_id: int
    pickup_datetime: datetime.datetime
    dropoff_datetime: datetime.datetime
    trip_distance: float
    pickup_location_id: int
    dropoff_location_id: int
    payment_type_id: int
    fare_amount: float
    extra_surcharge: float
    mta_tax: float
    improvement_surcharge: float
    tolls_amount: float
    tip_amount: float
    total_amount: float
    rate_code_id: int
    store_and_fwd_flag: bool
    _table_name = "taxi_trip"


@dataclass(order=True)
class TaxiTripFeatures(BaseDataClass):
    id: int
    taxi_trip_id: int
    trip_duration: datetime.timedelta
    avg_speed: float
    _table_name = "taxi_trip_features"
