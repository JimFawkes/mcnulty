CREATE Table IF NOT EXISTS locations (
    id                  INT PRIMARY KEY,
    borough             VARCHAR(40),
    zone                VARCHAR(40),
    service_zone        VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS payment_types (
    id                  INT PRIMARY KEY,
    name                VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_codes (
    id                  INT PRIMARY KEY,
    name                VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS location_coordinates (
    id                  INT PRIMARY KEY,
    location_id         INT REFERENCES locations (id),
    latitude            FLOAT NOT NULL,
    longitude           FLOAT NOT NULL,
    UNIQUE (latitude, longitude)
);

CREATE TABLE IF NOT EXISTS weather (
    id                      INT PRIMARY KEY,
    station_id              VARCHAR(32),
    station_name            VARCHAR(64),
    date                    DATE UNIQUE,
    temperature_min         FLOAT,
    temperature_max         FLOAT,
    temperature_avg         FLOAT,
    percipitation           FLOAT CHECK (percipitation >= 0),
    windspeed_avg           FLOAT CHECK (windspeed_avg >= 0),
    snow                    FLOAT CHECK (snow >= 0),
    snow_depth              FLOAT CHECK (snow_depth >= 0),
    sunshine_duration       FLOAT CHECK (sunshine_duration >= 0)
);

CREATE TABLE IF NOT EXISTS taxi_trip (
    id                      BIGSERIAL PRIMARY KEY,
    vendor_id               INT NOT NULL,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP NOT NULL,
    trip_distance           FLOAT CHECK (trip_distance >= 0) NOT NULL,
    pickup_location_id      INT REFERENCES locations (id) NOT NULL,
    dropoff_location_id     INT REFERENCES locations (id) NOT NULL,
    payment_type_id         INT REFERENCES payment_types (id) NOT NULL,
    fare_amount             FLOAT CHECK (fare_amount >= 0) NOT NULL,
    extra_surcharge         FLOAT CHECK (extra_surcharge >= 0) NOT NULL,
    mta_tax                 FLOAT CHECK (mta_tax >= 0) NOT NULL,
    improvement_surcharge   FLOAT CHECK (improvement_surcharge >= 0) NOT NULL,
    tolls_amount            FLOAT CHECK (tolls_amount >= 0) NOT NULL, 
    tip_amount              FLOAT CHECK (tip_amount >= 0) NOT NULL, 
    total_amount            FLOAT CHECK (total_amount >= 0) NOT NULL, 
    rate_code_id            INT REFERENCES rate_codes (id),
    store_and_fwd_flag      BOOL NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi_trip_features (
    id                  BIGSERIAL PRIMARY KEY,
    taxi_trip_id        BIGINT REFERENCES taxi_trip (id),
    trip_duration       INTERVAL,
    avg_speed           FLOAT CHECK (avg_speed >= 0) NOT NULL
);

CREATE TABLE IF NOT EXISTS taxi_trip_file_downloads (
    id                      SERIAL PRIMARY KEY,
    filename                VARCHAR(256) NOT NULL UNIQUE,
    datetime_processed      TIMESTAMP NOT NULL,
    is_processed            BOOL DEFAULT false
)