\echo 'Clearing reviews table'
TRUNCATE reviews CASCADE;

\echo 'Loading reviews from processed_data/reviews_chunk_*.csv'
\copy reviews (review_id, hotel_id, review_text) FROM PROGRAM 'python3 /app/scripts/emit_reviews_csv.py' WITH (FORMAT text);

\echo 'Reviews table stats:'
SELECT COUNT(*) AS total_reviews FROM reviews;
SELECT review_id, hotel_id, LEFT(review_text, 120) AS sample_text FROM reviews ORDER BY review_id LIMIT 5;