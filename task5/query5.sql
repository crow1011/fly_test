SELECT * 
FROM 
crosstab($$SELECT flightnumber, passengertype, COUNT(passengertype) FROM flights GROUP BY flightnumber,passengertype ORDER BY flightnumber, passengertype$$, 
	$$SELECT DISTINCT passengertype FROM flights ORDER BY passengertype$$) 
AS ct(id text, ADT text, CHD text, INF text);
