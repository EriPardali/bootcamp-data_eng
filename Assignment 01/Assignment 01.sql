-- CREATE TABLE actors (
--    actorid TEXT,
--    actor TEXT,
--    quality_class quality_class,
--    is_active BOOLEAN,
--     PRIMARY KEY (actorid)
-- );
--
-- CREATE TABLE films
-- (
--     filmid  INTEGER,
--     film TEXT,
--     actorid TEXT,
--     year     INTEGER,
--     votes    INTEGER,
--     rating   REAL,
--     PRIMARY KEY (filmid),
--     FOREIGN KEY (actorid) REFERENCES actors (actorid)
-- );

SELECT MAX(year) FROM actor_films;

-- CREATE TYPE quality_class AS ENUM('star','good', 'average', 'bad');

WITH actor_stats AS (
  SELECT
    actorid AS actor_id,
    actor AS actor_name,
    AVG(rating) AS avg_rating,
    TRUE AS is_active
  FROM actor_films
  WHERE year = 2021
  GROUP BY actorid, actor
)
INSERT INTO actors (actorid, actor, quality_class, is_active)
SELECT
  actor_id,
  actor_name,
  CASE
    WHEN avg_rating > 8 THEN 'star'::quality_class
    WHEN avg_rating > 7 THEN 'good'::quality_class
    WHEN avg_rating > 6 THEN 'average'::quality_class
    ELSE 'bad'
  END AS quality_class,
  is_active
FROM actor_stats;

-- CREATE TABLE actors_history_scd (
--     actorid TEXT,
--     actor TEXT,
--     quality_class quality_class,
--     is_active BOOLEAN,
--     start_date DATE,
--     end_date DATE,
--     PRIMARY KEY (actorid, start_date)
-- );

-- CREATE TABLE actors_scd (
--     actorid TEXT,
--     actor TEXT,
--     quality_class quality_class,
--     is_active BOOLEAN,
--     start_year INTEGER,
--     end_year INTEGER,
--     current_year INTEGER,
--     PRIMARY KEY (actorid, current_year)
-- );
WITH yearly_stats AS (
    SELECT
        actorid,
        actor,
        year,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year BETWEEN 1970 AND 2021
    GROUP BY actorid, actor, year
)
INSERT INTO actors_history_scd (
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
)
SELECT
    actorid,
    actor,
    CASE
        WHEN avg_rating > 8 THEN 'star'::quality_class
        WHEN avg_rating > 7 THEN 'good'::quality_class
        WHEN avg_rating > 6 THEN 'average'::quality_class
        ELSE 'bad'::quality_class
    END AS quality_class,
    TRUE AS is_active,
    TO_DATE(year || '-01-01', 'YYYY-MM-DD') AS start_date,
    TO_DATE(year || '-01-01', 'YYYY-MM-DD') AS end_date
FROM yearly_stats;

WITH params AS (
    SELECT 2021 AS current_year
),
current_stats AS (
    SELECT
        f.actorid,
        f.actor,
        CASE
            WHEN AVG(f.rating) > 8 THEN 'star'::quality_class
            WHEN AVG(f.rating) > 7 THEN 'good'::quality_class
            WHEN AVG(f.rating) > 6 THEN 'average'::quality_class
            ELSE 'bad'::quality_class
        END AS quality_class,
        TRUE AS is_active,
        TO_DATE(p.current_year || '-01-01', 'YYYY-MM-DD') AS start_date,
        TO_DATE(p.current_year || '-12-31', 'YYYY-MM-DD') AS end_date
    FROM actor_films f
    CROSS JOIN params p
    WHERE f.year = p.current_year
    GROUP BY f.actorid, f.actor, p.current_year
),
previous_stats AS (
    SELECT DISTINCT ON (actorid)
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE end_date < (
        SELECT TO_DATE(current_year || '-01-01', 'YYYY-MM-DD') FROM params
    )
    ORDER BY actorid, end_date DESC
),
changed_actors AS (
    SELECT
        c.actorid,
        c.actor,
        c.quality_class,
        c.is_active,
        c.start_date,
        c.end_date
    FROM current_stats c
    LEFT JOIN previous_stats p
        ON c.actorid = p.actorid
    WHERE p.actorid IS NULL
       OR c.quality_class <> p.quality_class
       OR c.is_active <> p.is_active
),
updated_previous AS (
    UPDATE actors_history_scd
    SET end_date = (
        SELECT TO_DATE(current_year || '-01-01', 'YYYY-MM-DD') - INTERVAL '1 day'
        FROM params
    )
    WHERE (actorid, end_date) IN (
        SELECT p.actorid, p.end_date
        FROM changed_actors c
        JOIN previous_stats p ON c.actorid = p.actorid
    )
    RETURNING *
)
-- Τελικό insert
INSERT INTO actors_history_scd (
    actorid, actor, quality_class, is_active, start_date, end_date
)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM changed_actors;

-- TRIAL
SELECT * FROM actors_history_scd
WHERE actorid = 'nm0000380'
ORDER BY start_date;