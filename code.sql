/*
SQL code for data Processing of sample Cyclistic data by Aniekan Ekanem

Processing Stage
While using Big Query to process the dataset, out of the 12 data that was downloaded, I was only able to work with 6 data since Big Query can only allow data size of less than 100MB.  Using appropriate file naming conventions, the following are the date files studied:
    • bike_sharing_2022_nov
    • bike_sharing_2022_dec
    • bike_sharing_2023_jan
    • bike_sharing_2023_feb
    • bike_sharing_2023_mar
    • bike_sharing_2023_apr

The tool I will use for processing the data is SQL because it is handy for working with larger datasets and provides more functionalities.  Another tool I could use is R but I will stick with SQL for now.

After cross checking data in accordance with the credibility check, I have been able to confirm the data's integrity.
*/

-- DATA INSPECTION AND CLEANING
-- a. Steps taken to ensure data is clean using SQL

-- i. Checking for duplicates:
SELECT COUNT(*) AS duplicate_rows_count
FROM (
   SELECT *, COUNT(*) AS count
   FROM `motivate_int_inc.bike_sharing_2022_nov`
   GROUP BY	ride_id, 
rideable_type, 
started_at, 
ended_at, 
start_station_name, 
start_station_id, 
end_station_name, 
end_station_id, 
start_lat, 
start_lng, 
end_lat, 
end_lng, 
member_casual,
   HAVING COUNT(*) > 1
) 
AS duplicates;

-- The above code was carried out for the remaining 5 data
---------------------------------------------------------------------------

-- ii. Checking for the presence of “NA” string in the tables submitted:

SELECT *
FROM `motivate_int_inc.bike_sharing_2023_mar`
WHERE CONCAT(
ride_id, 
rideable_type, 
started_at, 
ended_at, 
start_station_name, 
start_station_id, 
end_station_name, 
end_station_id, start_lat, 
start_lng, 
end_lat, 
end_lng, 
Member_casual
)
LIKE '%NA%'
---------------------------------------------------------------------------

-- DATA PROCESSING PROPER
-- b. Calculating the length of each ride as ‘ride_length_hms’.
SELECT *,
    FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(TIMESTAMP_DIFF(ended_at, started_at, SECOND))) AS ride_length_hms
FROM `motivate_int_inc.bike_sharing_2022_nov`
---------------------------------------------------------------------------

-- c. Calculating the days of the week to know which days each bike-sharing took place as well as the corresponding days based on the day value
SELECT *,
 EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
 CASE
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'sunday'
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'monday'
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'tuesday'
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'wednesday'
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'thursday'
   WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'friday'
   ELSE 'saturday'
 END AS starting_day
FROM `motivate_int_inc.bike_sharing_2022_nov`;
---------------------------------------------------------------------------

/*
4. Analysis
    a. Analysis using SQL
        i. Getting the number of Cyclistic members and casual riders by month period
*/
-- For Cyclistic members
SELECT member_casual, COUNT(*) AS count
FROM `motivate_int_inc.bike_sharing_2022_nov`
WHERE member_casual = 'member'
GROUP BY member_casual;
---------------------------------------------------------------------------

-- For Casual riders
SELECT member_casual, COUNT(*) AS count
FROM `motivate_int_inc.bike_sharing_2022_nov`
WHERE member_casual = casual
GROUP BY member_casual;
---------------------------------------------------------------------------

/*
The above code was also used to get the number of Cyclistic members and Casual riders for other month period.

  ii. Average ride_length for members and casual riders using SQL
*/
-- for Cyclistic members
SELECT
 FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(AVG(TIMESTAMP_DIFF(ended_at, started_at, SECOND)) AS INT64))) AS avg_ride_length_hms
FROM `motivate_int_inc.bike_sharing_2022_nov`
WHERE member_casual = 'member';
---------------------------------------------------------------------------

-- for casual riders
SELECT
 FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(AVG(TIMESTAMP_DIFF(ended_at, started_at, SECOND)) AS INT64))) AS avg_ride_length_hms
FROM `motivate_int_inc.bike_sharing_2022_nov`
WHERE member_casual = casual;
---------------------------------------------------------------------------

-- iii. Calculating the maximum ride length
SELECT
FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(MAX(TIMESTAMP_DIFF(ended_at, started_at, SECOND)))) AS max_ride_length_hms
FROM `motivate_int_inc.bike_sharing_2022_nov`;
---------------------------------------------------------------------------

-- iv. Getting the mode day of the week
WITH daily_counts AS (
 SELECT EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        COUNT(*) AS count
 FROM `motivate_int_inc.bike_sharing_2022_nov`
 GROUP BY day_of_week
),
max_count AS (
 SELECT MAX(count) AS max_count
 FROM daily_counts
)
SELECT day_of_week, starting_day
FROM (
 SELECT EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        CASE
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'sunday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'monday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'tuesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'wednesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'thursday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'friday'
          ELSE 'saturday'
        END AS starting_day,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
 FROM `motivate_int_inc.bike_sharing_2022_nov`
 GROUP BY day_of_week, starting_day
) subquery
WHERE rn = 1
---------------------------------------------------------------------------

--  v. Calculating the number of rides for users based on the days of the week by counting the number of ride_ids (combine the 6 tables for this)
SELECT starting_day, COUNT(ride_id) AS ride_count
FROM (
 SELECT ride_id,
        EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        CASE
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'sunday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'monday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'tuesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'wednesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'thursday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'friday'
          ELSE 'saturday'
        END AS starting_day
 FROM `motivate_int_inc.bike_sharing_2022_nov`
) subquery
GROUP BY starting_day
---------------------------------------------------------------------------

/*
COMBINED TABLES
  b. processing of data from the combined tables
I combined the 6 tables together to form one table of data using SQL.  Find below the code:
*/
CREATE TABLE `motivate_int_inc.divvy_tripdata_combined2` AS
SELECT * FROM `motivate_int_inc.bike_sharing_2022_nov`
UNION ALL
SELECT * FROM `motivate_int_inc.bike_sharing_2022_dec`
UNION ALL
SELECT * FROM `motivate_int_inc.bike_sharing_2023_jan`
UNION ALL
SELECT * FROM `motivate_int_inc.bike_sharing_2023_feb`
UNION ALL
SELECT * FROM `motivate_int_inc.bike_sharing_2023_mar`
UNION ALL
SELECT * FROM `motivate_int_inc.bike_sharing_2023_apr`;
---------------------------------------------------------------------------

--  i. Calculating the length of each ride as ‘ride_length_hms’.
		SELECT *,
            FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(TIMESTAMP_DIFF(ended_at, started_at, SECOND))) AS ride_length_hms
        FROM `motivate_int_inc.divvy_tripdata_combined2`
---------------------------------------------------------------------------

--  ii. Calculating the days of the week to know which days each bike-sharing took place as well as the corresponding days based on the day value
        SELECT *,
            EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'sunday'
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'monday'
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'thursday'
            WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'friday'
        ELSE 'saturday'
        END AS starting_day
        FROM `motivate_int_inc.divvy_tripdata_combined2`;
---------------------------------------------------------------------------

/*
 iii. Getting the number of Cyclistic members and casual riders
 */
-- For Cyclistic members
SELECT member_casual, COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = 'member'
GROUP BY member_casual;
---------------------------------------------------------------------------

-- ## For Casual riders
SELECT member_casual, COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = 'casual'
GROUP BY member_casual;
---------------------------------------------------------------------------

/*
iv. Calculating total rides by user type
*/
-- for member
SELECT  COUNT(ride_id)
FROM `my-data-project-24723.motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = 'member'
---------------------------------------------------------------------------

-- for casual
SELECT  COUNT(ride_id)
FROM `my-data-project-24723.motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = casual
---------------------------------------------------------------------------

/*
v. Calculating the total rides by bike type
*/
-- for classic_bike
SELECT  COUNT(rideable_type)
FROM `my-data-project-24723.motivate_int_inc.divvy_tripdata_combined2`
WHERE rideable_type = 'classic_bike'
---------------------------------------------------------------------------

-- for electric_bike
SELECT  COUNT(rideable_type)
FROM `my-data-project-24723.motivate_int_inc.divvy_tripdata_combined2`
WHERE rideable_type = 'electric_bike'
---------------------------------------------------------------------------

-- for docked_bike
SELECT  COUNT(rideable_type)
FROM `my-data-project-24723.motivate_int_inc.divvy_tripdata_combined2`
WHERE rideable_type = 'docked_bike'
---------------------------------------------------------------------------

/*
vi. Calculating the average ride length by user type
*/
-- for members
SELECT
FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(AVG(TIMESTAMP_DIFF(ended_at, started_at, SECOND)) AS INT64))) AS avg_ride_length_hms
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = 'member';
---------------------------------------------------------------------------

-- for casual riders
SELECT
FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(AVG(TIMESTAMP_DIFF(ended_at, started_at, SECOND)) AS INT64))) AS avg_ride_length_hms
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE member_casual = casual;
---------------------------------------------------------------------------

/*
vii. Getting the ride count amount by starting days
*/
SELECT starting_day, COUNT(ride_id) AS ride_count
FROM (
 SELECT ride_id,
        EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        CASE
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'sunday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'monday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'tuesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'wednesday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'thursday'
          WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'friday'
          ELSE 'saturday'
        END AS starting_day
 FROM `motivate_int_inc.bike_sharing_2022_nov`
) subquery
GROUP BY starting_day
---------------------------------------------------------------------------

/*
viii. Calculating the total rides by months
*/
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(MONTH FROM started_at) = 1;
-- (1 for Jan, 2 for feb, 3 for mar and so on)
---------------------------------------------------------------------------

/*
ix. Calculating the total riders:
*/
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
-- Result = 1585555
---------------------------------------------------------------------------

/*
x. Calculating average ride length:
*/
SELECT
FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(AVG(TIMESTAMP_DIFF(ended_at, started_at, SECOND)) AS INT64))) AS avg_ride_length_hms
FROM `motivate_int_inc.divvy_tripdata_combined2`
-- Result: 00:14:31
---------------------------------------------------------------------------

/*
xi. Getting the busiest time:
*/
-- for morning hours
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(HOUR FROM started_at) >= 6 AND EXTRACT(HOUR FROM started_at) < 12;
-- Result: 434286
---------------------------------------------------------------------------

-- for afternoon hours
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(HOUR FROM started_at) >= 12 AND EXTRACT(HOUR FROM started_at) < 18;
--Result;  714686
---------------------------------------------------------------------------

-- for evening hours
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(HOUR FROM started_at) >= 18 AND EXTRACT(HOUR FROM started_at) < 6;
-- Result;  0
---------------------------------------------------------------------------

/*
xii. Getting the busiest weekday:
*/
-- for sundays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 1
-- Result: 176711
---------------------------------------------------------------------------

-- for mondays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 2
-- Result: 204207
---------------------------------------------------------------------------

-- for tuesdays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 3
-- Result: 259377
---------------------------------------------------------------------------

-- for tuesdays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 4
-- Result: 260082
---------------------------------------------------------------------------

-- for tuesdays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 5
-- Result: 259749
---------------------------------------------------------------------------

-- for tuesdays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 6
-- Result: 226383
---------------------------------------------------------------------------

-- for tuesdays
SELECT COUNT(*) AS count
FROM `motivate_int_inc.divvy_tripdata_combined2`
WHERE EXTRACT(DAYOFWEEK FROM started_at) = 7
-- Result: 199046
---------------------------------------------------------------------------

/*
xiii. Getting the busiest season:  There are four seasons which are winter, summer, spring and fall.  

Winter does occur during the months of December, January, and February in the Northern Hemisphere, and also occurs during June, July, and August in the Southern Hemisphere.  In this context, Cyclistic is based in Chicago, and Chicago is located in the Northern Hemisphere, hence there will be winter in the months of December, January, and February.  For other seasons, the following applies:
Spring: March, April, May
Summer: June, July, August
Fall: September, October, November
Based on these, the busiest season will be gotten from the busiest months which has been deduced from the code in viii above.  April being the busiest happens to be part of the period where Spring occurs.  This means the busiest season is Spring.
*/