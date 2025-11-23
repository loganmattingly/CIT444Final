"""Load project schema and CSV data into PostgreSQL."""
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Iterable

import psycopg

SCHEMA_FILE = Path("database/schema_postgres.sql")
PROCESSED_DATA_DIR = Path("processed_data")


class PostgresLoader:
    def __init__(self) -> None:
        self.host = os.getenv("POSTGRES_HOST", "postgres")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "cit444")
        self.user = os.getenv("POSTGRES_USER", "cit444")
        self.password = os.getenv("POSTGRES_PASSWORD", "cit444")
        self.conn: psycopg.Connection | None = None

    # ------------------------------------------------------------------
    def connect(self) -> None:
        if self.conn is None:
            self.conn = psycopg.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
            )
            self.conn.execute("SET client_encoding TO 'UTF8'")

    # ------------------------------------------------------------------
    def run_schema(self) -> None:
        if not SCHEMA_FILE.exists():
            raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE}")

        sql_text = SCHEMA_FILE.read_text(encoding="utf-8")
        statements = _split_sql_statements(sql_text)

        with self.conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        self.conn.commit()
        print("✅ Schema applied successfully")

    # ------------------------------------------------------------------
    def load_hotels(self) -> None:
        hotels_csv = PROCESSED_DATA_DIR / "hotels.csv"
        if not hotels_csv.exists():
            print(f"⚠️ Skipping hotels load, file missing: {hotels_csv}")
            return

        insert_sql = (
            "INSERT INTO hotels (hotel_id, name, city, country, source_folder) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (hotel_id) DO UPDATE SET "
            "name = EXCLUDED.name, "
            "city = EXCLUDED.city, "
            "country = EXCLUDED.country, "
            "source_folder = EXCLUDED.source_folder"
        )

        with hotels_csv.open("r", encoding="utf-8") as fh, self.conn.cursor() as cur:
            reader = csv.DictReader(fh)
            count = 0
            for row in reader:
                cur.execute(
                    insert_sql,
                    (
                        _to_int(row.get("HOTELID")),
                        row.get("NAME"),
                        row.get("CITY"),
                        row.get("COUNTRY"),
                        row.get("SOURCE_FOLDER"),
                    ),
                )
                count += 1
            self.conn.commit()
        print(f"✅ Loaded {count} hotels")

    # ------------------------------------------------------------------
    def load_reviews(self) -> None:
        chunk_files = sorted(PROCESSED_DATA_DIR.glob("reviews_chunk_*.csv"))
        if not chunk_files:
            print("⚠️ No review chunk files found")
            return

        insert_sql = (
            "INSERT INTO reviews (review_id, hotel_id, review_text, file_source, line_number) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (review_id) DO UPDATE SET "
            "hotel_id = EXCLUDED.hotel_id, "
            "review_text = EXCLUDED.review_text, "
            "file_source = EXCLUDED.file_source, "
            "line_number = EXCLUDED.line_number"
        )

        total = 0
        for chunk_path in chunk_files:
            with chunk_path.open("r", encoding="utf-8") as fh, self.conn.cursor() as cur:
                reader = csv.DictReader(fh)
                line_no = 1
                for row in reader:
                    cur.execute(
                        insert_sql,
                        (
                            _to_int(row.get("IDREVIEW")),
                            _to_int(row.get("HOTELID")),
                            row.get("REVIEW"),
                            chunk_path.name,
                            line_no,
                        ),
                    )
                    total += 1
                    line_no += 1
            self.conn.commit()
            print(f"  ✓ {chunk_path.name}: {line_no - 1} reviews")
        print(f"✅ Loaded {total} reviews from {len(chunk_files)} chunks")

    # ------------------------------------------------------------------
    def load_ratings(self) -> None:
        ratings_csv = _pick_first_existing(
            PROCESSED_DATA_DIR / "final_ratings.csv",
            PROCESSED_DATA_DIR / "final_ratings_simple.csv",
        )
        if not ratings_csv:
            print("⚠️ No ratings CSV found (expected final_ratings*.csv)")
            return

        insert_sql = (
            "INSERT INTO ratings (review_id, hotel_id, service_score, price_score, "
            "room_score, location_score, overall_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (review_id) DO UPDATE SET "
            "hotel_id = EXCLUDED.hotel_id, "
            "service_score = EXCLUDED.service_score, "
            "price_score = EXCLUDED.price_score, "
            "room_score = EXCLUDED.room_score, "
            "location_score = EXCLUDED.location_score, "
            "overall_score = EXCLUDED.overall_score"
        )

        with ratings_csv.open("r", encoding="utf-8") as fh, self.conn.cursor() as cur:
            reader = csv.DictReader(fh)
            count = 0
            for row in reader:
                cur.execute(
                    insert_sql,
                    (
                        _to_int(row.get("REVIEWID")),
                        _to_int(row.get("HOTELID")),
                        _to_int(row.get("SERVICE")) or _to_int(row.get("SERVICE_SCORE")),
                        _to_int(row.get("PRICE")) or _to_int(row.get("PRICE_SCORE")),
                        _to_int(row.get("ROOM")) or _to_int(row.get("ROOM_SCORE")),
                        _to_int(row.get("LOCATION")) or _to_int(row.get("LOCATION_SCORE")),
                        _to_int(row.get("OVERALL")) or _to_int(row.get("OVERALL_SCORE")),
                    ),
                )
                count += 1
            self.conn.commit()
        print(f"✅ Loaded {count} ratings from {ratings_csv.name}")

    # ------------------------------------------------------------------
    def refresh_rating_averages(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE ratings_average")
            cur.execute(
                """
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
                GROUP BY hotel_id
                """
            )
        self.conn.commit()
        print("✅ ratings_average table refreshed")

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # ------------------------------------------------------------------
    def run(self) -> None:
        self.connect()
        self.run_schema()
        self.load_hotels()
        self.load_reviews()
        self.load_ratings()
        self.refresh_rating_averages()
        print("🎉 Postgres load complete")


def _split_sql_statements(sql_text: str) -> Iterable[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_string = False

    for char in sql_text:
        if char == "'":
            in_string = not in_string
        if char == ";" and not in_string:
            stmt = "".join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []
        else:
            buffer.append(char)
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _to_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return None


def _pick_first_existing(*candidates: Path) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


if __name__ == "__main__":
    loader = PostgresLoader()
    try:
        loader.run()
    finally:
        loader.close()
