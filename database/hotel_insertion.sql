\echo 'Loading hotels from processed_data/hotels.csv'
\copy hotels (hotel_id, name, city, country) FROM '/app/processed_data/hotels.csv' WITH (FORMAT csv, HEADER true);

\echo 'Hotels table row count:'
SELECT COUNT(*) FROM hotels;
