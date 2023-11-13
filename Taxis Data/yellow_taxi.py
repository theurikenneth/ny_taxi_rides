def yellow_taxi():
    yellow_taxi_data = """
    select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount, tip_amount, tolls_amount, total_amount, congestion_surcharge
    from trips_data_all.green_tripdata
    where store_and_fwd_flag = 'Y'
    """
    return yellow_taxi_data