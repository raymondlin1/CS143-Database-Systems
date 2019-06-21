/*
Raymond Lin
304937942
CS 143
*/

/*Homework 1 Part 1*/
/*a)*/
SELECT EXTRACT(HOUR FROM datetime) AS hour, SUM(throughput) AS trips
FROM hw1.rides2017
GROUP BY hour;

/*b)*/
SELECT origin, destination
FROM hw1.rides2017
WHERE EXTRACT(DOW FROM datetime) IN (1, 2, 3, 4, 5)
GROUP BY origin, destination
ORDER BY SUM(throughput) DESC
LIMIT 1;

/*c)*/
SELECT destination, AVG(throughput)
FROM hw1.rides2017
WHERE EXTRACT(DOW FROM datetime) IN (1) AND EXTRACT(HOUR FROM datetime) IN (7, 8, 9)
GROUP BY destination
ORDER BY AVG(throughput) DESC
LIMIT 5;