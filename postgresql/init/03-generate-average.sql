-- Generate train and expected averages from the converted measurements table.

CREATE OR REPLACE PROCEDURE generate_plug_average(
    p_window_size INTEGER,
    p_interval    INTERVAL
)
LANGUAGE plpgsql
AS
$$
DECLARE
    train_end_timestamp BIGINT := 1377986401 + 3 * 86400;
BEGIN
    RAISE NOTICE 'Generating plug average (% minutes)...', p_window_size;

    INSERT INTO plug_data (
        house_id, household_id, plug_id, year, month, day,
        slice_gap, slice_index, value, count, avg
    )
    WITH grouped_data AS (
        SELECT
            house_id,
            household_id,
            plug_id,
            value,
            timestamp AS sample_time
        FROM measurements
        WHERE timestamp < to_timestamp(train_end_timestamp)
    ), bucketed AS (
        SELECT
            house_id,
            household_id,
            plug_id,
            value,
            to_char(sample_time, 'YYYY') AS year,
            to_char(sample_time, 'MM') AS month,
            to_char(sample_time, 'DD') AS day,
            floor(
                extract(epoch FROM sample_time - date_trunc('day', sample_time))
                / extract(epoch FROM p_interval)
            )::INTEGER AS slice_index
        FROM grouped_data
    )
    SELECT
        house_id, household_id, plug_id, year, month, day,
        p_window_size, slice_index,
        sum(value), count(*)::DOUBLE PRECISION, avg(value)
    FROM bucketed
    GROUP BY house_id, household_id, plug_id, year, month, day, slice_index
    ON CONFLICT DO NOTHING;

    INSERT INTO plug_data_expected (
        house_id, household_id, plug_id, year, month, day,
        slice_gap, slice_index, avg
    )
    WITH grouped_data AS (
        SELECT
            house_id,
            household_id,
            plug_id,
            value,
            timestamp AS sample_time
        FROM measurements
        WHERE timestamp >= to_timestamp(train_end_timestamp)
    ), bucketed AS (
        SELECT
            house_id,
            household_id,
            plug_id,
            value,
            to_char(sample_time, 'YYYY') AS year,
            to_char(sample_time, 'MM') AS month,
            to_char(sample_time, 'DD') AS day,
            floor(
                extract(epoch FROM sample_time - date_trunc('day', sample_time))
                / extract(epoch FROM p_interval)
            )::INTEGER AS slice_index
        FROM grouped_data
    )
    SELECT
        house_id, household_id, plug_id, year, month, day,
        p_window_size, slice_index, avg(value)
    FROM bucketed
    GROUP BY house_id, household_id, plug_id, year, month, day, slice_index
    ON CONFLICT DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE generate_house_average(p_window_size INTEGER)
LANGUAGE plpgsql
AS
$$
BEGIN
    RAISE NOTICE 'Generating house average (% minutes)...', p_window_size;

    INSERT INTO house_data (
        house_id, year, month, day, slice_gap, slice_index, avg
    )
    SELECT
        house_id, year, month, day, slice_gap, slice_index, sum(avg)
    FROM plug_data
    WHERE slice_gap = p_window_size
    GROUP BY house_id, year, month, day, slice_gap, slice_index
    ON CONFLICT DO NOTHING;

    INSERT INTO house_data_expected (
        house_id, year, month, day, slice_gap, slice_index, avg
    )
    SELECT
        house_id, year, month, day, slice_gap, slice_index, sum(avg)
    FROM plug_data_expected
    WHERE slice_gap = p_window_size
    GROUP BY house_id, year, month, day, slice_gap, slice_index
    ON CONFLICT DO NOTHING;
END;
$$;

CALL generate_plug_average(1,   INTERVAL '1 minute');
CALL generate_plug_average(5,   INTERVAL '5 minutes');
CALL generate_plug_average(15,  INTERVAL '15 minutes');
CALL generate_plug_average(60,  INTERVAL '60 minutes');
CALL generate_plug_average(120, INTERVAL '120 minutes');

CALL generate_house_average(1);
CALL generate_house_average(5);
CALL generate_house_average(15);
CALL generate_house_average(60);
CALL generate_house_average(120);

DROP PROCEDURE generate_plug_average(INTEGER, INTERVAL);
DROP PROCEDURE generate_house_average(INTEGER);
DROP TABLE measurements;
