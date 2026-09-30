CREATE OR REPLACE PROCEDURE generate_forecast(p_window_size INTEGER)
LANGUAGE plpgsql
AS
$$
BEGIN
    RAISE NOTICE 'Generating expected forecast (% minutes)...', p_window_size;

    INSERT INTO plug_data_forecast_expected (
        house_id, household_id, plug_id, year, month, day,
        slice_gap, slice_index, avg
    )
    WITH historical AS (
        SELECT
            house_id,
            household_id,
            plug_id,
            slice_gap,
            slice_index,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY avg) AS median_avg
        FROM plug_data
        WHERE slice_gap = p_window_size
        GROUP BY house_id, household_id, plug_id, slice_gap, slice_index
    )
    SELECT
        expected.house_id,
        expected.household_id,
        expected.plug_id,
        expected.year,
        expected.month,
        expected.day,
        expected.slice_gap,
        expected.slice_index + 2,
        (expected.avg + historical.median_avg) / 2.0
    FROM plug_data_expected AS expected
    JOIN historical
      ON historical.house_id = expected.house_id
     AND historical.household_id = expected.household_id
     AND historical.plug_id = expected.plug_id
     AND historical.slice_gap = expected.slice_gap
     AND historical.slice_index = expected.slice_index + 2
    WHERE expected.slice_gap = p_window_size
    ON CONFLICT DO NOTHING;

    INSERT INTO house_data_forecast_expected (
        house_id, year, month, day, slice_gap, slice_index, avg
    )
    WITH historical AS (
        SELECT
            house_id,
            slice_gap,
            slice_index,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY avg) AS median_avg
        FROM house_data
        WHERE slice_gap = p_window_size
        GROUP BY house_id, slice_gap, slice_index
    )
    SELECT
        expected.house_id,
        expected.year,
        expected.month,
        expected.day,
        expected.slice_gap,
        expected.slice_index + 2,
        (expected.avg + historical.median_avg) / 2.0
    FROM house_data_expected AS expected
    JOIN historical
      ON historical.house_id = expected.house_id
     AND historical.slice_gap = expected.slice_gap
     AND historical.slice_index = expected.slice_index + 2
    WHERE expected.slice_gap = p_window_size
    ON CONFLICT DO NOTHING;
END;
$$;

CALL generate_forecast(1);
CALL generate_forecast(5);
CALL generate_forecast(15);
CALL generate_forecast(60);
CALL generate_forecast(120);

DROP PROCEDURE generate_forecast(INTEGER);
