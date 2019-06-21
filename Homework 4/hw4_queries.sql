/*Raymond Lin
304937942
CS143*/

/*Part 1*/
/*Method 0*/
EXPLAIN
SELECT
	highway,
	area,
	COUNT(DISTINCT EXTRACT(DOY FROM reported)) * 100 / 365 AS percentage_of_days_closed_365,
	COUNT(DISTINCT EXTRACT(DOY FROM reported)) * 100 / 353 AS percentage_of_days_closed_353
FROM hw2.caltrans
WHERE condition LIKE '%CLOSED%DUE TO SNOW%' OR condition LIKE '%CLOSED%FOR THE WINTER%'
GROUP BY highway, area
ORDER BY percentage_of_days_closed_365 DESC;

/*Method 1*/
EXPLAIN
SELECT
  highway,
  stretch,
  COUNT(1) AS days_closed,
  100 * COUNT(1) / 365 AS pct_closed_365,
  100 * COUNT(1) / 353 AS pct_closed_353
FROM (
  SELECT
     highway AS highway,
     area AS stretch,
     DATE(reported) AS closure
  FROM hw2.caltrans
  WHERE condition LIKE '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
  GROUP BY highway, stretch, closure
) result 
GROUP BY highway, stretch 
ORDER BY pct_closed_365 DESC;

/*Method 2*/
EXPLAIN
SELECT
  closures.highway,
  stretch,
  COUNT(1) AS days_closed,
  100 * COUNT(1) / 365 AS pct_closed_365,
  100 * COUNT(1) / 353 AS pct_closed_353
FROM (
  SELECT
     c.highway AS highway,
     c.area AS stretch,
     DATE(c.reported) AS closure
  FROM hw2.caltrans c
JOIN (
  SELECT
     DISTINCT highway, 
     area
  FROM hw2.caltrans
  WHERE condition like '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO NOW%')) snow_highways 
ON c.highway = snow_highways.highway
  WHERE condition like '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
  GROUP BY c.highway, stretch, closure
) closures
GROUP BY closures.highway, closures.stretch
ORDER BY pct_closed_365 DESC;

/*Method 3*/
EXPLAIN
SELECT
  highway,
  stretch,
  COUNT(1) AS days_closed,
  100 * COUNT(1) / 365 AS pct_closed_365,
  100 * COUNT(1) / 353 AS pct_closed_353
FROM (
  SELECT
     c.highway AS highway,
     c.area AS stretch,
     DATE(c.reported) AS closure
  FROM hw2.caltrans c
  WHERE (highway, area) IN (
     SELECT
        DISTINCT highway,
        area
     FROM hw2.caltrans
     WHERE condition like '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')) 
        AND condition LIKE '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
     GROUP BY highway, stretch, closure
  ) closures
GROUP BY highway, stretch
ORDER BY pct_closed_365 DESC;

/*Method 4*/
EXPLAIN
SELECT
  highway,
  stretch,
  COUNT(1) AS days_closed,
  100 * COUNT(1) / 365 AS pct_closed_365,
  100 * COUNT(1) / 353 AS pct_closed_353
FROM (
  SELECT
     c.highway AS highway,
     c.area AS stretch,
     DATE(c.reported) AS closure
  FROM hw2.caltrans c
  WHERE EXISTS (
     SELECT
        1
     FROM hw2.caltrans
     WHERE condition like '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')) 
        AND condition LIKE '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
     GROUP BY highway, stretch, closure
  ) closures
GROUP BY highway, stretch
ORDER BY pct_closed_365 DESC;