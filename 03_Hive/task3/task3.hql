SELECT hotel_continent, hotel_country, hotel_market, COUNT(*) count
FROM train WHERE is_booking == 0
GROUP BY hotel_continent, hotel_country, hotel_market
ORDER BY count DESC LIMIT 3;