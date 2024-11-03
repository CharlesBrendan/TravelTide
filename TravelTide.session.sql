--This CTE prelimits our sessions on Elena's suggested timeframe (After Jan 4 2023)

  WITH sessions_2023 AS (

  SELECT *
  FROM sessions s
  WHERE s.session_start >= '2023-01-04'

),

-- This CTE returns the ids of all users with more than 7 sessions in 2023
filtered_users AS (

  SELECT user_id,
  			 COUNT(*)
  FROM sessions_2023 s
  GROUP BY user_id
  HAVING COUNT(*) >= 8

),

session_base AS (

 SELECT
  		s.session_id,
  		s.user_id,
  		s.trip_id,
  		s.session_start,
  		s.session_end,
  		s.page_clicks,
 			s.flight_discount,
  		s.flight_discount_amount,
  		s.hotel_discount,
  		s.hotel_discount_amount,
  		s.flight_booked,
  		CASE
  			WHEN s.flight_booked = 'yes' THEN 1
  			ELSE 0
  		END AS flight_booked_int,
  		s.hotel_booked,
  		CASE
  			WHEN s.hotel_booked = 'yes' THEN 1
  			ELSE 0
  		END AS hotel_booked_int,
  		s.cancellation,
  		CASE
  			WHEN s.cancellation = 'yes' THEN 1
  			ELSE 0
  		END AS cancellation_int,
 			u.birthdate,
  		u.gender,
  		u.married,
  		u.has_children,
  		u.home_country,
  		u.home_city,
  		u.home_airport,
  		u.home_airport_lat,
  		u.home_airport_lon,
  		u.sign_up_date,
 			f.origin_airport,
  		f.destination,
  		f.destination_airport,
  		f.seats,
  		f.return_flight_booked,
  		f.departure_time,
  		f.return_time,
  		f.checked_bags,
  		f.trip_airline,
  		f.destination_airport_lat,
  		f.destination_airport_lon,
  		f.base_fare_usd,
 			h.hotel_name,
  		CASE
  			WHEN h.nights < 0 THEN 1
  			ELSE h.nights
  		END AS nights,
  		h.rooms,
  		h.check_in_time,
  		h.check_out_time,
  		h.hotel_per_room_usd AS hotel_price_per_room_night_usd

  FROM sessions_2023 s

  LEFT JOIN users u
		ON s.user_id = u.user_id
	LEFT JOIN flights f
		ON s.trip_id = f.trip_id
	LEFT JOIN hotels h
		ON s.trip_id = h.trip_id

  WHERE s.user_id IN (SELECT user_id FROM filtered_users)

),


-- This CTE returns the ids of all trips that have been canceled through a session
-- We use this list to filter all canceled sessions in the next CTE

canceled_trips AS (

  SELECT DISTINCT trip_id
  FROM session_base
  WHERE cancellation = TRUE

),

-- This is our second base table to aggregate later
-- It is derived from our session_base table, but we focus on valid trips

-- All sessions without trips, all canceled trips have been removed
-- Each row represents a trip that a user did

not_canceled_trips AS(

  SELECT *
  FROM session_base
	WHERE trip_id IS NOT NULL
	AND trip_id NOT IN (SELECT trip_id FROM canceled_trips)

),


-- We want to aggregate user behaviour into metrics (a row per user)
-- This CTE contains metrics that have to do with the browsing behaviour
-- ALL SESSION within our cohort get aggregated

user_base_session AS(

		SELECT user_id,
  	SUM(page_clicks) AS num_clicks,
  	COUNT(DISTINCT session_id) AS num_sessions,

--avg flight booked
  	COUNT(flight_booked_int) AS flight_booked,

--Hotels booked
  	COUNT(hotel_booked_int) AS hotel_booked,

  --avg fare
    ROUND(AVG(base_fare_usd), 2) AS avg_flight_price,

  --Average night booked
    ROUND(AVG(nights),2) AS avg_nights,

  --bookng rate
		ROUND((COUNT(DISTINCT(trip_id)) * 1.0) / COUNT(session_id), 2) AS booking_rate,

  --Number of cancellations
    SUM(cancellation_int) AS num_cancellations,

--cancellation rate
   CASE WHEN COUNT(DISTINCT(trip_id)) > 0 THEN ROUND(SUM(cancellation_int) * 1.0 / COUNT(DISTINCT(trip_id)), 2)
                                                   ELSE 0 END AS cancellation_rate,

  --Average session duration in minutes
  	ROUND(ABS(EXTRACT(EPOCH FROM AVG(session_start - session_end)) / 60),2) AS avg_session_duration_min
    FROM session_base
    GROUP BY user_id

),

-- We want to aggregate user behaviour into metrics (a row per user)
-- This CTE contains metrics that have to do with the travel behavious
-- Only rows with VALID trips within our cohort get aggregated

	user_base_trip AS(

    SELECT 	user_id,
    			 	COUNT(DISTINCT trip_id) AS num_trips,
            SUM(CASE
                  WHEN (flight_booked = TRUE) AND (return_flight_booked = TRUE) THEN 2
                  WHEN flight_booked = TRUE THEN 1 ELSE 0
                END) AS num_flights,
            COALESCE((SUM((hotel_price_per_room_night_usd * nights * rooms) *
                          (1 - (CASE
                                  WHEN hotel_discount_amount IS NULL THEN 0
                                  ELSE hotel_discount_amount
                                END)))),0) AS money_spend_hotel,
--Average seats booked
          SUM(seats) AS num_booked_seats,

-- flight discount
    CAST(ROUND(AVG(flight_discount_amount), 2) AS FLOAT) AS avg_flight_discount,

--avg hotel discount
    CAST(ROUND(AVG(hotel_discount_amount), 2) AS FLOAT) AS avg_hotel_discount,

--Average trip duration in days
    ROUND(AVG(EXTRACT(DAY FROM(return_time - departure_time))),2) AS avg_trip_duration,

-- average number of rooms in booked hotels
    	ROUND(AVG(rooms),2) AS avg_hotel_rooms_booked,

--Average waiting time after flight booking
      ROUND(AVG(EXTRACT(DAY FROM departure_time-session_end)), 2) AS time_after_booking,

--Average checked bags
    	ROUND(SUM(checked_bags) * 1.0 / COUNT(DISTINCT(trip_id)), 2) AS avg_checked_bags,

--Calculating average distance flown (km) using haverine equation
           ROUND(AVG(CAST(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon) AS NUMERIC)),2) AS avg_km_flown
    FROM not_canceled_trips
		GROUP BY user_id
)

-- For our final user table, we join the session metric, trip metrics and general user information
-- Using a left join, we will get a row for each user from our original cohort codition (7+ browsing sessions in 2023)
-- If we used an inner join, we could get rid of users that have not actually travelled

SELECT b.*,
			 EXTRACT(YEAR FROM AGE(u.birthdate)) AS age,
       u.gender,
       u.married,
       u.has_children,
       u.home_country,
       u.home_city,
       u.home_airport,
			 t.*

FROM user_base_session b
	LEFT JOIN users u
		ON b.user_id = u.user_id
	LEFT JOIN user_base_trip t
		ON b.user_id = t.user_id