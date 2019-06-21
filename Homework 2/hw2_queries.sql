/*
Raymond Lin
304937942
CS 143
*/

/*Part 1*/
/*a)*/
SELECT DISTINCT highway, area
FROM hw2.caltrans
WHERE condition LIKE '% CLOSED % SNOW %' OR condition LIKE '% CLOSED % WINTER %'
ORDER BY highway, area
LIMIT 20;

/*b)*/
SELECT 
	stretch,
	closed_days * 100 / (COUNT(DISTINCT EXTRACT (DOY FROM hw2.caltrans.reported)))::double precision AS percentage
FROM (
		SELECT 
			COUNT(DISTINCT EXTRACT (DOY FROM reported))::double precision AS closed_days, 
			(highway, area) AS stretch
		FROM hw2.caltrans
		WHERE (condition LIKE '% CLOSED % SNOW %' OR condition LIKE '% CLOSED % WINTER %')
		GROUP BY stretch
	) sq,
	hw2.caltrans
WHERE sq.stretch = stretch
GROUP BY stretch, closed_days
ORDER BY percentage DESC
LIMIT 5;

/*Part 3*/
/*a)*/
SELECT 
	trip_start.trip_id,
	trip_start.user_id,
	CEIL(EXTRACT(EPOCH FROM age(COALESCE(hw2.trip_end.time, hw2.trip_start.time + make_interval(days := 1)), hw2.trip_start.time)) / 60)
		AS trip_length
FROM hw2.trip_start LEFT OUTER JOIN hw2.trip_end
	ON hw2.trip_end.trip_id = hw2.trip_start.trip_id
	AND hw2.trip_end.user_id = hw2.trip_start.user_id
LIMIT 5;

/*b)*/
SELECT 
	trip_start.trip_id,
	trip_start.user_id,
	1 + 0.15*(CEIL(EXTRACT(EPOCH FROM age(COALESCE(hw2.trip_end.time, hw2.trip_start.time + make_interval(days := 1)), hw2.trip_start.time)) / 60))
		AS trip_charge
FROM hw2.trip_start LEFT OUTER JOIN hw2.trip_end
	ON hw2.trip_end.trip_id = hw2.trip_start.trip_id
	AND hw2.trip_end.user_id = hw2.trip_start.user_id
LIMIT 5;

/*c)*/
SELECT 
	trip_start.user_id,
	SUM (
		CASE WHEN 
			(1 + 0.15*
				CEIL
				(
					(
					EXTRACT
						(
						EPOCH FROM age
							(
							COALESCE(hw2.trip_end.time, hw2.trip_start.time + make_interval(days := 1)), hw2.trip_start.time
							)
						) / 60
					)
				)
			) > 100 THEN 100
			ELSE 
			(1 + 0.15*
				CEIL
				(
					(
					EXTRACT
						(
						EPOCH FROM age
							(
							COALESCE(hw2.trip_end.time, hw2.trip_start.time + make_interval(days := 1)), hw2.trip_start.time
							)
						) / 60
					)
				)
			) 
		END
		)
		AS monthly_charge
FROM hw2.trip_start LEFT OUTER JOIN hw2.trip_end
	ON hw2.trip_end.trip_id = hw2.trip_start.trip_id
	AND hw2.trip_end.user_id = hw2.trip_start.user_id
WHERE (EXTRACT(MONTH FROM hw2.trip_start.time) IN (3)) AND (EXTRACT(YEAR FROM hw2.trip_start.time) IN (2018))
GROUP BY trip_start.user_id
LIMIT 5;

