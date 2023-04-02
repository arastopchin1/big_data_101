CREATE TABLE IF NOT EXISTS Train(
    date_time DATE,
    site_name INT,
    posa_continent INT,
    user_location_country INT,
    user_location_region INT,
    user_location_city INT,
    orig_destination_distance FLOAT,
    user_id INT,
    is_mobile TINYINT,
    is_package TINYINT,
    channel INT,
    srch_ci DATE,
    srch_co DATE,
    srch_adults_cnt TINYINT,
    srch_children_cnt TINYINT,
    srch_rm_cnt TINYINT,
    srch_destination_id INT,
    srch_destination_type_id INT,
    is_booking TINYINT,
    cnt INT,
    hotel_continent INT,
    hotel_country INT,
    hotel_market INT,
    hotel_cluster INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;