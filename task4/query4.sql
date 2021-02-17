SELECT ROUND(passengerscount/10.0+0.49,0) as cut_by_10, COUNT(flightnumber) as  flightnumber_count
FROM flights_2
GROUP BY  cut_by_10
ORDER BY cut_by_10;
