-- =============================================
-- Hospitality Analysis Setup Script (MySQL)
-- =============================================

-- ======================
-- 0. Create & Use Database
-- ======================
CREATE DATABASE IF NOT EXISTS hospitality;
USE hospitality;

-- =========================
-- 1. Drop old tables/views
-- =========================
DROP VIEW IF EXISTS 
  kpi_total_revenue,
  kpi_total_bookings,
  kpi_cancellation_rate,
  kpi_occupancy,
  kpi_utilized_capacity,
  kpi_trend_analysis,
  kpi_weekday_vs_weekend,
  kpi_revenue_by_city_hotel,
  kpi_classwise_revenue,
  kpi_booking_status_counts;

DROP TABLE IF EXISTS 
  fact_bookings, fact_aggregated_bookings,
  dim_hotels, dim_rooms, dim_date,
  stg_bookings, stg_aggregated_bookings,
  stg_hotels, stg_rooms, stg_date;

-- ======================
-- 2. Staging Tables
-- ======================
CREATE TABLE stg_hotels (
  property_id VARCHAR(20),
  property_name VARCHAR(100),
  category VARCHAR(50),
  city VARCHAR(50)
);

CREATE TABLE stg_rooms (
  room_id VARCHAR(20),
  room_class VARCHAR(50)
);

CREATE TABLE stg_bookings (
  booking_id VARCHAR(20),
  property_id VARCHAR(20),
  booking_date VARCHAR(20),
  check_in_date VARCHAR(20),
  checkout_date VARCHAR(20),
  no_guests VARCHAR(20),
  room_category VARCHAR(50),
  booking_platform VARCHAR(50),
  ratings_given VARCHAR(20),
  booking_status VARCHAR(20),
  revenue_generated VARCHAR(20),
  revenue_realized VARCHAR(20)
);

CREATE TABLE stg_aggregated_bookings (
  property_id VARCHAR(20),
  check_in_date VARCHAR(20),
  room_category VARCHAR(50),
  successful_bookings VARCHAR(20),
  capacity VARCHAR(20)
);

CREATE TABLE stg_date (
  full_date VARCHAR(20),
  mmm_yy VARCHAR(10),
  week_no VARCHAR(10),
  day_type VARCHAR(20)
);

-- ======================
-- 3. Load Data into Staging 
-- ======================


-- Hotels
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_hotels.csv'
INTO TABLE stg_hotels
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Rooms
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_rooms.csv'
INTO TABLE stg_rooms
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Bookings
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fact_bookings.csv'
INTO TABLE stg_bookings
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Aggregated Bookings
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fact_aggregated_bookings.csv'
INTO TABLE stg_aggregated_bookings
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Date
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_date.csv'
INTO TABLE stg_date
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@date, mmm_yy, week_no, day_type)
SET full_date = @date;


-- ======================
-- 4. Final Tables
-- ======================
CREATE TABLE dim_hotels (
  property_id INT PRIMARY KEY,
  property_name VARCHAR(100),
  category VARCHAR(50),
  city VARCHAR(50)
);

CREATE TABLE dim_rooms (
  room_id VARCHAR(20) PRIMARY KEY,   -- RT1, RT2 etc.
  room_class VARCHAR(50)
);

CREATE TABLE fact_bookings (
  booking_id VARCHAR(50) PRIMARY KEY,
  property_id INT,
  booking_date DATE,
  check_in_date DATE,
  checkout_date DATE,
  no_guests INT,
  room_category VARCHAR(50),
  booking_platform VARCHAR(50),
  ratings_given DECIMAL(3,2),
  booking_status VARCHAR(20),
  revenue_generated DECIMAL(12,2),
  revenue_realized DECIMAL(12,2),
  FOREIGN KEY (property_id) REFERENCES dim_hotels(property_id)
);

CREATE TABLE fact_aggregated_bookings (
  property_id INT,
  check_in_date DATE,
  room_category VARCHAR(50),
  successful_bookings INT,
  capacity INT,
  FOREIGN KEY (property_id) REFERENCES dim_hotels(property_id)
);

CREATE TABLE dim_date (
  full_date DATE PRIMARY KEY,
  mmm_yy VARCHAR(10),
  week_no VARCHAR(10),
  day_type VARCHAR(20)
);

-- ======================
-- 5. Transform Staging → Final
-- ======================
INSERT INTO dim_hotels
SELECT CAST(property_id AS UNSIGNED),
       property_name,
       category,
       city
FROM stg_hotels;

INSERT INTO dim_rooms
SELECT 
    room_id,          -- RT1, RT2
    room_class     -- Standard, Elite, etc.
FROM stg_rooms;

TRUNCATE fact_bookings;

INSERT INTO fact_bookings (
    booking_id,
    property_id,
    booking_date,
    check_in_date,
    checkout_date,
    no_guests,
    room_category,
    booking_platform,
    ratings_given,
    booking_status,
    revenue_generated,
    revenue_realized
)
SELECT 
    booking_id,
    CAST(property_id AS UNSIGNED),
    CASE
        WHEN booking_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(TRIM(booking_date), '%Y-%m-%d')
        WHEN booking_date LIKE '__/__/____'
            THEN STR_TO_DATE(TRIM(booking_date), '%d/%m/%Y')
        WHEN booking_date LIKE '__-__-____'
            THEN STR_TO_DATE(TRIM(booking_date), '%d-%m-%Y')
        ELSE NULL
    END AS booking_date,
    CASE
        WHEN check_in_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(TRIM(check_in_date), '%Y-%m-%d')
        WHEN check_in_date LIKE '__/__/____'
            THEN STR_TO_DATE(TRIM(check_in_date), '%d/%m/%Y')
        WHEN check_in_date LIKE '__-__-____'
            THEN STR_TO_DATE(TRIM(check_in_date), '%d-%m-%Y')
        ELSE NULL
    END AS check_in_date,
    CASE
        WHEN checkout_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(TRIM(checkout_date), '%Y-%m-%d')
        WHEN checkout_date LIKE '__/__/____'
            THEN STR_TO_DATE(TRIM(checkout_date), '%d/%m/%Y')
        WHEN checkout_date LIKE '__-__-____'
            THEN STR_TO_DATE(TRIM(checkout_date), '%d-%m-%Y')
        ELSE NULL
    END AS checkout_date,
    CAST(no_guests AS UNSIGNED),
    room_category,
    booking_platform,
    CAST(NULLIF(ratings_given,'') AS DECIMAL(3,2)),
    TRIM(booking_status),
    CAST(NULLIF(revenue_generated,'') AS DECIMAL(12,2)),
    CAST(NULLIF(revenue_realized,'') AS DECIMAL(12,2))
FROM stg_bookings;


INSERT INTO fact_aggregated_bookings
SELECT property_id,
       STR_TO_DATE(check_in_date, '%d-%b-%y'),
       room_category,
       CAST(successful_bookings AS UNSIGNED),
       CAST(capacity AS UNSIGNED)
FROM stg_aggregated_bookings; 

INSERT INTO dim_date (full_date, mmm_yy, week_no, day_type)
SELECT STR_TO_DATE(TRIM(full_date), '%d-%b-%y'),
       mmm_yy,
       week_no,
       day_type
FROM stg_date;



-- ======================
-- 6. KPI Views
-- ======================

-- Total Revenue
CREATE OR REPLACE VIEW kpi_total_revenue AS
SELECT SUM(revenue_realized) AS total_revenue
FROM fact_bookings
WHERE booking_status = 'Checked Out';

-- Total Bookings
CREATE OR REPLACE VIEW kpi_total_bookings AS
SELECT COUNT(*) AS total_bookings
FROM fact_bookings;

-- Cancellation Rate
CREATE OR REPLACE VIEW kpi_cancellation_rate AS
SELECT 
  SUM(CASE WHEN booking_status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) 
  AS cancellation_rate_percent
FROM fact_bookings;

-- Occupancy %
CREATE OR REPLACE VIEW kpi_occupancy AS
SELECT 
  SUM(successful_bookings) * 100.0 / SUM(capacity) AS occupancy_percent
FROM fact_aggregated_bookings;

-- Utilized Capacity
CREATE OR REPLACE VIEW kpi_utilized_capacity AS
SELECT 
  SUM(successful_bookings) AS utilized_capacity
FROM fact_aggregated_bookings;

-- Trend Analysis
CREATE OR REPLACE VIEW kpi_trend_analysis AS
SELECT 
    d.mmm_yy,
    SUM(fb.revenue_realized) AS total_revenue,
    COUNT(fb.booking_id) AS total_bookings
FROM fact_bookings fb
JOIN dim_date d 
  ON fb.booking_date = d.full_date
WHERE LOWER(TRIM(fb.booking_status)) = 'checked out'
GROUP BY d.mmm_yy
ORDER BY STR_TO_DATE(d.mmm_yy, '%b %y');


-- Weekday vs Weekend
CREATE OR REPLACE VIEW kpi_weekday_vs_weekend AS
SELECT 
    d.day_type,
    SUM(fb.revenue_realized) AS total_revenue,
    COUNT(fb.booking_id) AS total_bookings
FROM fact_bookings fb
JOIN dim_date d 
  ON fb.booking_date = d.full_date
WHERE fb.booking_status = 'Checked Out'
GROUP BY d.day_type;

-- Revenue by City & Hotel
CREATE OR REPLACE VIEW kpi_revenue_by_city_hotel AS
SELECT 
    h.city,
    h.property_name,
    SUM(fb.revenue_realized) AS revenue
FROM fact_bookings fb
JOIN dim_hotels h 
  ON fb.property_id = h.property_id
WHERE fb.booking_status = 'Checked Out'
GROUP BY h.city, h.property_name
ORDER BY h.city, revenue DESC;

-- Class-wise Revenue
CREATE OR REPLACE VIEW kpi_classwise_revenue AS
SELECT 
    dr.room_class,
    SUM(fb.revenue_realized) AS revenue
FROM fact_bookings fb
JOIN dim_rooms dr 
    ON fb.room_category = dr.room_id
WHERE LOWER(TRIM(fb.booking_status)) = 'checked out'
GROUP BY dr.room_class
ORDER BY revenue DESC;


-- Booking Status Counts
CREATE OR REPLACE VIEW kpi_booking_status_counts AS
SELECT 
    booking_status,
    COUNT(*) AS count_bookings
FROM fact_bookings
GROUP BY booking_status;


-- Weekly Trend Analysis
CREATE OR REPLACE VIEW kpi_weekly_trend_analysis AS
SELECT 
    d.week_no,
    SUM(fb.revenue_realized) AS total_revenue,
    COUNT(fb.booking_id) AS total_bookings
FROM fact_bookings fb
JOIN dim_date d 
  ON fb.booking_date = d.full_date
WHERE LOWER(TRIM(fb.booking_status)) = 'checked out'
GROUP BY d.week_no;



SELECT * FROM kpi_total_revenue;
SELECT * FROM kpi_total_bookings;
SELECT * FROM kpi_cancellation_rate;
SELECT * FROM kpi_occupancy;
SELECT * FROM kpi_trend_analysis;
SELECT * FROM kpi_weekday_vs_weekend;
SELECT * FROM kpi_revenue_by_city_hotel LIMIT 50;
SELECT * FROM kpi_classwise_revenue;
SELECT * FROM kpi_booking_status_counts;
SELECT * FROM kpi_utilized_capacity;
SELECT * FROM kpi_weekly_trend_analysis;

