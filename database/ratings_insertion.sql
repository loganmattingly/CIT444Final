-- CIT444 Hotel Analysis - Ratings Data Insertion
SET SERVEROUTPUT ON;
SET FEEDBACK ON;

DECLARE
    fh_ratings UTL_FILE.FILE_TYPE;
    v_line VARCHAR2(4000);
    
    -- Rating fields
    v_reviewid ratings.IDREVIEW%TYPE;
    v_hotelid ratings.HOTELID%TYPE;
    v_service ratings.SERVICE_SCORE%TYPE;
    v_price ratings.PRICE_SCORE%TYPE;
    v_room ratings.ROOM_SCORE%TYPE;
    v_location ratings.LOCATION_SCORE%TYPE;
    v_overall ratings.OVERALL_SCORE%TYPE;
    
    v_rating_count NUMBER := 0;
    v_error_count NUMBER := 0;
    v_total_lines NUMBER := 0;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting ratings data insertion...');
    DBMS_OUTPUT.PUT_LINE('Reading from: final_ratings.csv');
    DBMS_OUTPUT.PUT_LINE('===================================================');

    -- Open file
    BEGIN
        fh_ratings := UTL_FILE.FOPEN('EXT_DIR', 'final_ratings.csv', 'R', 32767);
        DBMS_OUTPUT.PUT_LINE('File opened successfully');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error opening file: ' || SQLERRM);
            RETURN;
    END;

    -- Skip header row
    BEGIN
        UTL_FILE.GET_LINE(fh_ratings, v_line);
        DBMS_OUTPUT.PUT_LINE('Skipped header row');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error reading header: ' || SQLERRM);
    END;

    -- Process data rows
    LOOP
        BEGIN
            UTL_FILE.GET_LINE(fh_ratings, v_line);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                EXIT;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error reading line: ' || SQLERRM);
                EXIT;
        END;

        BEGIN
            -- Parse CSV (REVIEWID,HOTELID,SERVICE,PRICE,ROOM,LOCATION,OVERALL)
            v_reviewid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^([^,]+)', 1, 1, NULL, 1));
            v_hotelid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,([^,]+)', 1, 1, NULL, 1));
            v_service := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,([^,]+)', 1, 1, NULL, 1));
            v_price := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,([^,]+)', 1, 1, NULL, 1));
            v_room := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,[^,]*,([^,]+)', 1, 1, NULL, 1));
            v_location := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,([^,]+)', 1, 1, NULL, 1));
            v_overall := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,([^,]+)', 1, 1, NULL, 1));

            -- Validate data
            IF v_reviewid IS NOT NULL AND v_hotelid IS NOT NULL THEN
                -- Check if rating already exists
                DECLARE
                    v_exists NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO v_exists FROM ratings WHERE IDREVIEW = v_reviewid;
                    
                    IF v_exists = 0 THEN
                        INSERT INTO ratings (
                            RATINGID, IDREVIEW, HOTELID, 
                            SERVICE_SCORE, PRICE_SCORE, ROOM_SCORE, 
                            LOCATION_SCORE, OVERALL_SCORE
                        ) VALUES (
                            rating_seq.NEXTVAL, v_reviewid, v_hotelid,
                            v_service, v_price, v_room, v_location, v_overall
                        );
                        
                        v_rating_count := v_rating_count + 1;
                        
                        -- Commit in batches
                        IF MOD(v_rating_count, 100) = 0 THEN
                            COMMIT;
                            DBMS_OUTPUT.PUT_LINE('Committed ' || v_rating_count || ' ratings...');
                        END IF;
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('Skipping duplicate rating for review: ' || v_reviewid);
                        v_error_count := v_error_count + 1;
                    END IF;
                END;
                
            ELSE
                DBMS_OUTPUT.PUT_LINE('Invalid data in line: ' || v_line);
                v_error_count := v_error_count + 1;
            END IF;

        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                DBMS_OUTPUT.PUT_LINE('Duplicate rating (constraint): ' || v_reviewid);
                v_error_count := v_error_count + 1;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error inserting rating: ' || SQLERRM || ' | Line: ' || v_line);
                v_error_count := v_error_count + 1;
        END;
    END LOOP;

    -- Final commit
    COMMIT;
    UTL_FILE.FCLOSE(fh_ratings);

    -- Calculate and insert average ratings
    DBMS_OUTPUT.PUT_LINE('Calculating average ratings...');
    
    BEGIN
        INSERT INTO ratingsaverage (
            HOTELID, AVG_SERVICE, AVG_PRICE, AVG_ROOM, 
            AVG_LOCATION, AVG_OVERALL, TOTAL_REVIEWS
        )
        SELECT 
            HOTELID,
            ROUND(AVG(SERVICE_SCORE), 2),
            ROUND(AVG(PRICE_SCORE), 2),
            ROUND(AVG(ROOM_SCORE), 2),
            ROUND(AVG(LOCATION_SCORE), 2),
            ROUND(AVG(OVERALL_SCORE), 2),
            COUNT(*)
        FROM ratings
        GROUP BY HOTELID;
        
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Average ratings calculated for ' || SQL%ROWCOUNT || ' hotels.');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error calculating averages: ' || SQLERRM);
            ROLLBACK;
    END;

    DBMS_OUTPUT.PUT_LINE('===================================================');
    DBMS_OUTPUT.PUT_LINE('RATINGS DATA INSERTION COMPLETED');
    DBMS_OUTPUT.PUT_LINE('Total ratings inserted: ' || v_rating_count);
    DBMS_OUTPUT.PUT_LINE('Total errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Average ratings table updated.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error: ' || SQLERRM);
        ROLLBACK;
        BEGIN
            UTL_FILE.FCLOSE(fh_ratings);
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END;
/

-- Verify results
SELECT COUNT(*) AS total_ratings FROM ratings;
SELECT COUNT(*) AS hotels_with_average_ratings FROM ratingsaverage;

-- Display sample averages
SELECT * FROM ratingsaverage ORDER BY AVG_OVERALL DESC FETCH FIRST 5 ROWS ONLY;

-- Display view sample
SELECT * FROM hotel_ratings_view WHERE ROWNUM <= 5;
