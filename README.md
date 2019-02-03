# mcnulty
Project McNulty - Metis

## Task:
**Requirements:**
 - SQL Database (PostgreSQL)
 - Enhanced visualizations (Webapp/D3.js)
 - Classification Problem/Supervised Learning


**MVP:**
Classify NY Taxi Trips into `tip` and `no-tip` categories.

The New York City Taxi and Limusine Commission records every taxi ride and publishes this info. The MVP goal for this project is to predict weather or not a cab driver will get a tip for a ride given the pickup and dropoff location and possible other features.

In the very first draft, I will limit the data to only several months from 2016 and possibly only a subset of the rides, maybe only Yellow Cab.

A noteworthy point is, that the dataset only automatically records tips if the customer paid with credit card. If the customer paid with cash, it is up to the driver to enter the amount manually. So it might also be necessary to only look at credit card payments in the first draft.

**Data:**
NYC [TLC Trip Record Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) (January 2009 - June 2018)
The data is provided monthly in csv files for every category (Yellow, Green, FHV). The data consists of 141 files with sizes varying between 80MB and 2.4GB, 1m rows and 20m rows.
A first rough estimation of the entire dataset results in ~170GB with 1.4 billion rows.

Tables:
[Yellow Taxi](http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) (January 2009 - June 2018)
 - VendorID, 
 - pickup_datetime, 
 - dropoff_datetime, 
 - passenger_count, 
 - trip_distance, 
 - pickup_location_id, 
 - dropoff_location_id, 
 - rate_code_id, 
 - store_and_fwd_flag, 
 - payment_type, 
 - fare_amount, 
 - extra, 
 - mta_tax, 
 - improvement_surcharge, 
 - tip_amount, 
 - tolls_amount, 
 - total_amount

[Green Taxi](http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf) (August 2013 - June 2018)
 - VendorID, 
 - pickup_datetime, 
 - dropoff_datetime, 
 - passenger_count, 
 - trip_distance, 
 - pickup_location_id, 
 - dropoff_location_id, 
 - rate_code_id, 
 - store_and_fwd_flag, 
 - payment_type, 
 - fare_amount, 
 - extra, 
 - mta_tax, 
 - improvement_surcharge, 
 - tip_amount, 
 - tolls_amount, 
 - total_amount,
 - trip_type

[FHV](http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf) (January 2015 - June 2018)
 - Dispatching_base_num
 - Pickup_datetime
 - DropOff_datetime
 - PULocationID
 - DOLocationID
 - SR_Flag

[Taxi Zone Lookup](http://www.nyc.gov/html/exit-page.html?url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)
 - LocationID,
 - Borough,Zone,
 - service_zone

Weather
Daily summaries provided by [National Centers For Environmental Information](https://www.ncdc.noaa.gov/)

 - Temperature (Min,Max,AVG) - Celsius
 - Percipitation - mm
 - Sunshine duration - minutes
 - Snow - mm
 - Windspeed average - km/h

Possible Additinal Data:
Locations
 - type (business/residential)

**Docker:**
 - Image: jimfawkes/project-mcnulty
