CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating NUMERIC(3,1),
    filmid INTEGER
);
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
DROP TABLE actors;
CREATE TABLE actors (
    actor_id TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actor_id, current_year)
);



INSERT INTO actors
WITH last_year AS (
  SELECT * FROM actors WHERE current_year = 2018
),
this_year AS (
  SELECT
    actorid AS actor_id,
    ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
    AVG(rating) AS avg_rating,
    TRUE AS is_active,
	year
  FROM actor_films
  WHERE year = 2019
  GROUP BY actorid, year
)

SELECT
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  COALESCE(ly.films, ARRAY[]::films[])
  || CASE
      WHEN ty.films IS NOT NULL THEN ty.films
      ELSE ARRAY[]::films[]
     END AS films,
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
  ty.is_active IS NOT NULL AND ty.is_active AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year

FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id;