-- PostgreSQL schema for CIT444 project
DROP VIEW IF EXISTS hotel_ratings_view;
DROP TABLE IF EXISTS ratings_average CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS hotels CASCADE;

CREATE TABLE hotels (
    hotel_id BIGINT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    source_folder VARCHAR(300),
    created_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE reviews (
    review_id BIGINT NOT NULL,
    hotel_id BIGINT NOT NULL REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    review_text TEXT,
    file_source VARCHAR(150),
    line_number INTEGER,
    processed_date TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT pk_reviews PRIMARY KEY (hotel_id, review_id)
);

CREATE TABLE ratings (
    rating_id BIGSERIAL PRIMARY KEY,
    review_id BIGINT NOT NULL,
    hotel_id BIGINT NOT NULL REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    service_score SMALLINT CHECK (service_score BETWEEN 1 AND 5),
    price_score SMALLINT CHECK (price_score BETWEEN 1 AND 5),
    room_score SMALLINT CHECK (room_score BETWEEN 1 AND 5),
    location_score SMALLINT CHECK (location_score BETWEEN 1 AND 5),
    overall_score SMALLINT CHECK (overall_score BETWEEN 1 AND 5),
    analysis_date TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_ratings_review UNIQUE (hotel_id, review_id),
    CONSTRAINT fk_ratings_review FOREIGN KEY (hotel_id, review_id)
        REFERENCES reviews(hotel_id, review_id) ON DELETE CASCADE
);

CREATE TABLE ratings_average (
    hotel_id BIGINT PRIMARY KEY REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    avg_service NUMERIC(5,2),
    avg_price NUMERIC(5,2),
    avg_room NUMERIC(5,2),
    avg_location NUMERIC(5,2),
    avg_overall NUMERIC(5,2),
    total_reviews INTEGER,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_reviews_hotel_id ON reviews(hotel_id);
CREATE INDEX idx_ratings_hotel_id ON ratings(hotel_id);

CREATE OR REPLACE VIEW hotel_ratings_view AS
SELECT
    h.hotel_id,
    h.name AS hotel_name,
    h.city,
    h.country,
    ra.avg_service,
    ra.avg_price,
    ra.avg_room,
    ra.avg_location,
    ra.avg_overall,
    ra.total_reviews,
    ra.last_updated,
    CASE
        WHEN ra.avg_overall >= 4.5 THEN 'Excellent'
        WHEN ra.avg_overall >= 4.0 THEN 'Very Good'
        WHEN ra.avg_overall >= 3.0 THEN 'Good'
        WHEN ra.avg_overall >= 2.0 THEN 'Fair'
        ELSE 'Poor'
    END AS rating_category
FROM hotels h
LEFT JOIN ratings_average ra ON h.hotel_id = ra.hotel_id;
