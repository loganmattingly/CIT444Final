-- CIT444 Hotel Analysis - Hotel Data Insertion
SET SERVEROUTPUT ON;
SET FEEDBACK ON;

DECLARE
    fh UTL_FILE.FILE_TYPE;
    v_line VARCHAR2(4000);
    v_hotelid hotel.HOTELID%TYPE;
    v_name hotel.NAME%TYPE;
    v_city hotel.CITY%TYPE;
    v_country hotel.COUNTRY%TYPE;
    v_count NUMBER := 0;
    v_errors NUMBER := 0;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting hotel data insertion...');
    
    -- Open file
    BEGIN
        fh := UTL_FILE.FOPEN('EXT_DIR', 'hotels.csv', 'R');
        DBMS_OUTPUT.PUT_LINE('File opened successfully');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error opening file: ' || SQLERRM);
            RETURN;
    END;

    -- Skip header
    BEGIN
        UTL_FILE.GET_LINE(fh, v_line);
        DBMS_OUTPUT.PUT_LINE('Skipped header: ' || v_line);
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    -- Process data
    LOOP
        BEGIN
            UTL_FILE.GET_LINE(fh, v_line);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN EXIT;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error reading line: ' || SQLERRM);
                EXIT;
        END;

        BEGIN
            -- Parse CSV (HOTELID,NAME,CITY,COUNTRY)
            v_hotelid := NULL;
            v_name := NULL;
            v_city := NULL;
            v_country := NULL;
            
            -- Simple CSV parsing
            v_hotelid := TO_NUMBER(REGEXP_SUBSTR(v_line, '^([^,]+)', 1, 1, NULL, 1));
            v_name := REGEXP_SUBSTR(v_line, '^[^,]*,\s*"?([^,"]+)"?', 1, 1, NULL, 1);
            v_city := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,\s*"?([^,"]+)"?', 1, 1, NULL, 1);
            v_country := REGEXP_SUBSTR(v_line, '^[^,]*,[^,]*,[^,]*,\s*"?([^,"]+)"?', 1, 1, NULL, 1);

            -- Remove quotes if present
            IF v_name IS NOT NULL THEN
                v_name := REPLACE(v_name, '"', '');
            END IF;
            IF v_city IS NOT NULL THEN
                v_city := REPLACE(v_city, '"', '');
            END IF;
            IF v_country IS NOT NULL THEN
                v_country := REPLACE(v_country, '"', '');
            END IF;

            -- Insert if we have valid data
            IF v_hotelid IS NOT NULL AND v_name IS NOT NULL THEN
                INSERT INTO hotel (HOTELID, NAME, CITY, COUNTRY)
                VALUES (v_hotelid, v_name, v_city, v_country);
                v_count := v_count + 1;
                
                IF MOD(v_count, 50) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('Processed ' || v_count || ' hotels...');
                    COMMIT;
                END IF;
            ELSE
                v_errors := v_errors + 1;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                v_errors := v_errors + 1;
                DBMS_OUTPUT.PUT_LINE('Error processing line: ' || SQLERRM || ' | Line: ' || v_line);
        END;
    END LOOP;

    UTL_FILE.FCLOSE(fh);
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('✅ Hotel insertion completed');
    DBMS_OUTPUT.PUT_LINE('Total hotels inserted: ' || v_count);
    DBMS_OUTPUT.PUT_LINE('Total errors: ' || v_errors);

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error: ' || SQLERRM);
        BEGIN
            UTL_FILE.FCLOSE(fh);
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        ROLLBACK;
END;
/

-- Verify insertion
SELECT COUNT(*) AS total_hotels FROM hotel;
SELECT * FROM hotel ORDER BY HOTELID FETCH FIRST 5 ROWS ONLY;
