-- CIT444 Hotel Analysis - Hotel Data Insertion
SET SERVEROUTPUT ON;
SET FEEDBACK ON;
SET VERIFY OFF;

DECLARE
    fh_hotels    UTL_FILE.FILE_TYPE;
    v_line       VARCHAR2(4000);
    v_hotelid    hotel.HOTELID%TYPE;
    v_name       hotel.NAME%TYPE;
    v_city       hotel.CITY%TYPE;
    v_country    hotel.COUNTRY%TYPE;
    v_hotel_count NUMBER := 0;
    v_error_count NUMBER := 0;
    v_total_lines NUMBER := 0;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting hotel data insertion...');
    DBMS_OUTPUT.PUT_LINE('Reading from: hotels.csv');
    DBMS_OUTPUT.PUT_LINE('=' || RPAD('=', 50, '='));

    -- Count total lines first for progress tracking
    BEGIN
        fh_hotels := UTL_FILE.FOPEN('EXT_DIR', 'hotels.csv', 'r');
        
        -- Skip header
        UTL_FILE.GET_LINE(fh_hotels, v_line);
        
        WHILE TRUE LOOP
            BEGIN
                UTL_FILE.GET_LINE(fh_hotels, v_line);
                v_total_lines := v_total_lines + 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN EXIT;
            END;
        END LOOP;
        
        UTL_FILE.FCLOSE(fh_hotels);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error counting lines: ' || SQLERRM);
            v_total_lines := 0;
    END;

    -- Reopen file for data processing
    BEGIN
        fh_hotels := UTL_FILE.FOPEN('EXT_DIR', 'hotels.csv', 'r');
        DBMS_OUTPUT.PUT_LINE('File opened successfully.');
        
        -- Skip header row
        BEGIN
            UTL_FILE.GET_LINE(fh_hotels, v_line);
            DBMS_OUTPUT.PUT_LINE('Header skipped: ' || v_line);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error reading header: ' || SQLERRM);
        END;

        -- Process data rows
        v_hotel_count := 0;
        LOOP
            BEGIN
                UTL_FILE.GET_LINE(fh_hotels, v_line);
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    EXIT;
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error reading line: ' || SQLERRM);
                    EXIT;
            END;

            BEGIN
                -- Parse CSV line (simple comma separation)
                v_hotelid := NULL;
                v_name := NULL;
                v_city := NULL;
                v_country := NULL;
                
                -- Extract fields (assuming: HOTELID,NAME,CITY,COUNTRY)
                v_hotelid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^([^,]+)', 1, 1, NULL, 1));
                v_name := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,"?([^,"]+)"?', 1, 1, NULL, 1);
                v_city := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,"?([^,"]+)"?', 1, 1, NULL, 1);
                v_country := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,[^,]*,"?([^,"]+)"?', 1, 1, NULL, 1);
                
                -- Remove quotes if present
                v_name := REPLACE(v_name, '"', '');
                v_city := REPLACE(v_city, '"', '');
                v_country := REPLACE(v_country, '"', '');

                -- Validate required fields
                IF v_hotelid IS NOT NULL AND v_name IS NOT NULL AND v_city IS NOT NULL AND v_country IS NOT NULL THEN
                    -- Insert into hotel table
                    INSERT INTO hotel (HOTELID, NAME, CITY, COUNTRY)
                    VALUES (v_hotelid, v_name, v_city, v_country);
                    
                    v_hotel_count := v_hotel_count + 1;
                    
                    -- Progress update every 10 records
                    IF MOD(v_hotel_count, 10) = 0 THEN
                        DBMS_OUTPUT.PUT_LINE('Processed ' || v_hotel_count || ' hotels...');
                    END IF;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Skipping invalid record: ' || v_line);
                    v_error_count := v_error_count + 1;
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error processing record: ' || SQLERRM || ' | Line: ' || v_line);
                    v_error_count := v_error_count + 1;
            END;
        END LOOP;

        UTL_FILE.FCLOSE(fh_hotels);
        DBMS_OUTPUT.PUT_LINE('File closed.');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error opening file: ' || SQLERRM);
            BEGIN
                UTL_FILE.FCLOSE(fh_hotels);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
            RAISE;
    END;

    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('=' || RPAD('=', 50, '='));
    DBMS_OUTPUT.PUT_LINE('HOTEL DATA INSERTION COMPLETED');
    DBMS_OUTPUT.PUT_LINE('Total hotels inserted: ' || v_hotel_count);
    DBMS_OUTPUT.PUT_LINE('Total errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Commit successful.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- Verify insertion
SELECT COUNT(*) AS "Total Hotels in Database" FROM hotel;
SELECT * FROM hotel ORDER BY HOTELID FETCH FIRST 10 ROWS ONLY;