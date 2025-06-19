-- DDL for actors table:
DROP TYPE IF EXISTS films;

CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating NUMERIC(3,1),
    filmid INTEGER
);

DROP TYPE IF EXISTS quality_class;

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

DROP TABLE IF EXISTS actors;

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, actor, current_year)
);

-- Cumulative table generation query: 
INSERT INTO actors
WITH last_year AS (
  SELECT * FROM actors WHERE current_year = 2019
),
this_year AS (
  SELECT
    actorid,
    actor,
    ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
    AVG(rating) AS avg_rating,
    TRUE AS is_active,
    year
  FROM actor_films
  WHERE year = 2020
  GROUP BY actorid, actor, year
)
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actorid, ty.actorid) AS actorid,
  COALESCE(ly.films, ARRAY[]::films[]) || COALESCE(ty.films, ARRAY[]::films[]) AS films,
  CASE
    WHEN ty.avg_rating IS NOT NULL THEN
      CASE
        WHEN ty.avg_rating > 8 THEN 'star'::quality_class
        WHEN ty.avg_rating > 7 THEN 'good'::quality_class
        WHEN ty.avg_rating > 6 THEN 'average'::quality_class
        ELSE 'bad'::quality_class
      END
    ELSE ly.quality_class
  END AS quality_class,
  (ty.is_active IS NOT NULL AND ty.is_active) AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid;


-- DDL for actors_history_scd table:
DROP TABLE IF EXISTS actors_history_scd;

CREATE TABLE actors_history_scd (
    actorid TEXT,
    is_active BOOLEAN,
    quality_class quality_class,
    snapshot_year INTEGER,
    start_year INTEGER,
    end_year INTEGER
);

DROP TYPE IF EXISTS actors_scd_type;

CREATE TYPE actors_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);

-- Backfill actors_history_scd:
INSERT INTO actors_history_scd (
    actorid,
    is_active,
    quality_class,
    snapshot_year,
    start_year,
    end_year
)
WITH status_lagged AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_is_active
    FROM actors
    WHERE current_year <= 2021
),
status_changes AS (
    SELECT *,
        CASE
            WHEN quality_class IS DISTINCT FROM prev_quality_class
              OR is_active IS DISTINCT FROM prev_is_active
            THEN 1 ELSE 0
        END AS status_change
    FROM status_lagged
),
status_streaks AS (
    SELECT *,
        SUM(status_change) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
    FROM status_changes
)
SELECT
    actorid,
    is_active,
    quality_class,
    2024 AS snapshot_year,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year
FROM status_streaks
GROUP BY actorid, streak_id, is_active, quality_class
ORDER BY actorid, start_year;


-- Incremental query for actors_history_scd: 
DROP TYPE IF EXISTS actors_scd_type;

CREATE TYPE actors_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);

WITH last_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE end_year = 2021
),
historical_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE end_year < 2021
),
this_year_data AS (
    SELECT *
    FROM actors
    WHERE current_year = 2022
),
unchanged_records AS (
    SELECT
        ty.actorid,
        ty.is_active,
        ty.quality_class,
        ty.current_year AS snapshot_year,
        ls.start_year,
        ty.current_year AS end_year
    FROM this_year_data ty
    JOIN last_scd ls ON ty.actorid = ls.actorid
    WHERE ty.quality_class = ls.quality_class
      AND ty.is_active = ls.is_active
),
changed_records AS (
    SELECT
        ty.actorid,
        UNNEST(ARRAY[
            ROW(
                ls.quality_class,
                ls.is_active,
                ls.start_year,
                ls.end_year
            )::actors_scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
            )::actors_scd_type
        ]) AS record,
        ty.current_year AS snapshot_year
    FROM this_year_data ty
    LEFT JOIN last_scd ls ON ty.actorid = ls.actorid
    WHERE ty.quality_class IS DISTINCT FROM ls.quality_class
       OR ty.is_active IS DISTINCT FROM ls.is_active
),
unnested_changed_records AS (
    SELECT
        actorid,
        (record).is_active,
        (record).quality_class,
        snapshot_year,
        (record).start_year,
        (record).end_year
    FROM changed_records
),
new_records AS (
    SELECT
        ty.actorid,
        ty.is_active,
        ty.quality_class,
        ty.current_year AS snapshot_year,
        ty.current_year AS start_year,
        ty.current_year AS end_year
    FROM this_year_data ty
    LEFT JOIN last_scd ls ON ty.actorid = ls.actorid
    WHERE ls.actorid IS NULL
)

INSERT INTO actors_history_scd (
    actorid,
    is_active,
    quality_class,
    snapshot_year,
    start_year,
    end_year
)
SELECT * FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) final_data
ORDER BY actorid, start_year;

