-- CIT444 Hotel Analysis - Process Reviews Data
SET SERVEROUTPUT ON;
SET FEEDBACK ON;

DECLARE
    v_review_count NUMBER := 0;
    v_error_count NUMBER := 0;
    v_chunk_number NUMBER := 1;
    v_file_exists BOOLEAN := TRUE;
    fh_reviews UTL_FILE.FILE_TYPE;
    v_line VARCHAR2(4000);
    
    v_reviewid review.IDREVIEW%TYPE;
    v_hotelid review.HOTELID%TYPE;
    v_review_text review.REVIEW%TYPE;
    v_file_source review.FILE_SOURCE%TYPE;
    v_line_number review.LINE_NUMBER%TYPE;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting review data processing...');
    DBMS_OUTPUT.PUT_LINE('This will process all review chunk files.');
    DBMS_OUTPUT.PUT_LINE('=' || RPAD('=', 50, '='));

    -- Process each chunk file
    WHILE v_file_exists LOOP
        BEGIN
            -- Try to open the chunk file
            fh_reviews := UTL_FILE.FOPEN('EXT_DIR', 'reviews_chunk_' || v_chunk_number || '.csv', 'r');
            DBMS_OUTPUT.PUT_LINE('Processing reviews_chunk_' || v_chunk_number || '.csv');
            
            -- Skip header
            BEGIN
                UTL_FILE.GET_LINE(fh_reviews, v_line);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;

            -- Process data rows
            LOOP
                BEGIN
                    UTL_FILE.GET_LINE(fh_reviews, v_line);
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN EXIT;
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('Error reading line: ' || SQLERRM);
                        EXIT;
                END;

                BEGIN
                    -- Parse CSV (IDREVIEW,HOTELID,REVIEW)
                    v_reviewid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^([^,]+)', 1, 1, NULL, 1));
                    v_hotelid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^[^,]*,([^,]+)', 1, 1, NULL, 1));
                    
                    -- Extract review text (handle commas in text)
                    v_review_text := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,"?(.*)"?$', 1, 1, NULL, 1);
                    
                    -- Remove surrounding quotes if present
                    IF v_review_text LIKE '"%"' THEN
                        v_review_text := SUBSTR(v_review_text, 2, LENGTH(v_review_text) - 2);
                    END IF;

                    IF v_reviewid IS NOT NULL AND v_hotelid IS NOT NULL AND v_review_text IS NOT NULL THEN
                        INSERT INTO review (IDREVIEW, HOTELID, REVIEW, FILE_SOURCE)
                        VALUES (v_reviewid, v_hotelid, v_review_text, 'reviews_chunk_' || v_chunk_number || '.csv');
                        
                        v_review_count := v_review_count + 1;
                        
                        -- Progress update
                        IF MOD(v_review_count, 100) = 0 THEN
                            DBMS_OUTPUT.PUT_LINE('Processed ' || v_review_count || ' reviews...');
                            COMMIT;
                        END IF;
                    ELSE
                        v_error_count := v_error_count + 1;
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('Error processing review: ' || SQLERRM);
                        v_error_count := v_error_count + 1;
                END;
            END LOOP;

            UTL_FILE.FCLOSE(fh_reviews);
            v_chunk_number := v_chunk_number + 1;
            COMMIT;
            
        EXCEPTION
            WHEN UTL_FILE.INVALID_OPERATION OR UTL_FILE.INVALID_PATH THEN
                -- File doesn't exist, stop processing
                v_file_exists := FALSE;
                IF v_chunk_number = 1 THEN
                    DBMS_OUTPUT.PUT_LINE('No review chunk files found!');
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Processed all ' || (v_chunk_number - 1) || ' chunk files.');
                END IF;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error processing chunk ' || v_chunk_number || ': ' || SQLERRM);
                v_error_count := v_error_count + 1;
                BEGIN
                    UTL_FILE.FCLOSE(fh_reviews);
                EXCEPTION
                    WHEN OTHERS THEN NULL;
                END;
                v_chunk_number := v_chunk_number + 1;
        END;
    END LOOP;

    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('=' || RPAD('=', 50, '='));
    DBMS_OUTPUT.PUT_LINE('REVIEW DATA PROCESSING COMPLETED');
    DBMS_OUTPUT.PUT_LINE('Total reviews inserted: ' || v_review_count);
    DBMS_OUTPUT.PUT_LINE('Total errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Total chunk files processed: ' || (v_chunk_number - 1));

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- Verify review data
SELECT COUNT(*) AS "Total Reviews" FROM review;
SELECT * FROM review ORDER BY IDREVIEW FETCH FIRST 5 ROWS ONLY;