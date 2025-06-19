-- Create graph database schema
DROP TYPE IF EXISTS vertex_type CASCADE;
CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');

DROP TYPE IF EXISTS edge_type CASCADE;
CREATE TYPE edge_type AS ENUM (
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

DROP TABLE IF EXISTS vertices CASCADE;

CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

DROP TABLE IF EXISTS edges CASCADE;

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

-- Load game vertices
INSERT INTO vertices
SELECT 
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
    ) AS properties
FROM games;

-- Load player vertices with aggreg. stats
INSERT INTO vertices
WITH players_agg AS (
    SELECT 
        player_id AS identifier,
        MAX(player_name) AS player_name, 
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id
)
SELECT 
    identifier, 
    'player'::vertex_type AS type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    ) AS properties
FROM players_agg;

-- Load team vertices
INSERT INTO vertices
WITH teams_deduped AS (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY team_id) AS row_num
    FROM teams
)
SELECT 
    team_id AS identifier, 
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) AS properties
FROM teams_deduped
WHERE row_num = 1;

-- Create player-to-player relationships
INSERT INTO edges (
    subject_identifier,
    subject_type,
    object_identifier,
    object_type,
    edge_type,
    properties
)
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered AS (
    SELECT * FROM deduped
    WHERE row_num = 1
),
player_pairs AS (
    SELECT
        f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        f1.game_id,
        f1.team_abbreviation AS subject_team,
        f2.team_abbreviation AS object_team,
        f1.pts AS subject_points,
        f2.pts AS object_points
    FROM filtered f1
    JOIN filtered f2
        ON f1.game_id = f2.game_id
        AND f1.player_id <> f2.player_id
    WHERE f1.player_id > f2.player_id
),
aggregated AS (
    SELECT
        subject_player_id::TEXT AS subject_identifier,
        'player'::vertex_type AS subject_type,
        object_player_id::TEXT AS object_identifier,
        'player'::vertex_type AS object_type,
        CASE 
            WHEN subject_team = object_team THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        COUNT(1) AS num_games,
        SUM(subject_points) AS total_subject_points,
        SUM(object_points) AS total_object_points
    FROM player_pairs
    GROUP BY
        subject_player_id,
        object_player_id,
        CASE 
            WHEN subject_team = object_team THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
SELECT DISTINCT
    subject_identifier,
    subject_type,
    object_identifier,
    object_type,
    edge_type,
    jsonb_build_object(
        'num_games', num_games,
        'subject_points', total_subject_points,
        'object_points', total_object_points
    ) AS properties
FROM aggregated;

-- Create player-to-game relationships
INSERT INTO edges
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
)
SELECT 
    player_id AS subject_identifier, 
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) AS properties
FROM deduped
WHERE row_num = 1;

-- Check data loaded correctly
SELECT type, COUNT(1) AS count
FROM vertices
GROUP BY type;

SELECT 
    v.properties ->> 'player_name' AS player_name,
    MAX(CAST(e.properties ->> 'pts' AS INTEGER)) AS max_points_in_game
FROM vertices v 
JOIN edges e ON e.subject_identifier = v.identifier AND e.subject_type = v.type
WHERE v.type = 'player'::vertex_type AND e.edge_type = 'plays_in'::edge_type
GROUP BY v.properties ->> 'player_name'
ORDER BY max_points_in_game DESC
LIMIT 10;