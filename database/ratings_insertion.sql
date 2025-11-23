\echo 'Clearing ratings table'
TRUNCATE ratings RESTART IDENTITY;

\echo 'Loading ratings from processed_data/final_ratings.csv'
\copy ratings (review_id, hotel_id, service_score, price_score, room_score, location_score, overall_score) FROM '/app/processed_data/final_ratings.csv' WITH (FORMAT csv, HEADER true);

\echo 'Refreshing ratings_average table'
TRUNCATE ratings_average;
INSERT INTO ratings_average (
    hotel_id,
    avg_service,
    avg_price,
    avg_room,
    avg_location,
    avg_overall,
    total_reviews,
    last_updated
)
SELECT
    hotel_id,
    ROUND(AVG(service_score)::numeric, 2),
    ROUND(AVG(price_score)::numeric, 2),
    ROUND(AVG(room_score)::numeric, 2),
    ROUND(AVG(location_score)::numeric, 2),
    ROUND(AVG(overall_score)::numeric, 2),
    COUNT(*),
    NOW()
FROM ratings
GROUP BY hotel_id;

\echo 'Ratings summary'
SELECT COUNT(*) AS total_ratings FROM ratings;
SELECT COUNT(*) AS hotels_with_average_ratings FROM ratings_average;
