SELECT train.hotel_country, count(*) count
FROM train WHERE is_booking == 1
GROUP BY train.hotel_country
ORDER BY count DESC LIMIT 3;