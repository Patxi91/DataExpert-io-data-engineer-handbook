-- Check and create the type season_stats if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'season_stats') THEN
        CREATE TYPE season_stats AS (
            season INTEGER,
            pts REAL,
            ast REAL,
            reb REAL,
            weight INTEGER
        );
    END IF;
END $$;

-- Check and create the type scoring_class if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'scoring_class') THEN
        CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
    END IF;
END $$;

-- Create the table players if it doesn't exist
CREATE TABLE IF NOT EXISTS players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

-- Cumulative Table between Today and Yesterday
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1995 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1996
	)
SELECT * FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
-- Now COALESCE values that are not temporal, values that are not changing
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1995 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1996
	)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE WHEN y.season_stats IS NULL
		THEN ARRAY[ROW(
						t.season,
						t.gp,
						t.pts,
						t.ast,
						t.reb
						)::season_stats]
	ELSE y.season_stats || ARRAY[ROW(
						t.season,
						t.gp,
						t.pts,
						t.ast,
						t.reb
						)::season_stats]
	END
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
-- Now avoid todays Null values (player retires)
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1995 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1996
	)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE 
		WHEN y.season_stats IS NULL -- Create initial array w/ 1 value if null
			THEN ARRAY[ROW(
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW( -- Create new value if Today not null
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
	ELSE y.season_stats -- Carry history forward otherwise (retired player)
	END as season_stats,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
-- Turn the previous command into a little pipeline
DELETE FROM players; -- Idempotent
INSERT INTO players
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1995 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1996
	)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE 
		WHEN y.season_stats IS NULL -- Create initial array w/ 1 value if null
			THEN ARRAY[ROW(
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW( -- Create new value if Today not null
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
	ELSE y.season_stats -- Carry history forward otherwise (retired player)
	END as season_stats,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
-- Load Year by Year 2000-2001
INSERT INTO players
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 2000 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 2001
	)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE 
		WHEN y.season_stats IS NULL -- Create initial array w/ 1 value if null
			THEN ARRAY[ROW(
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW( -- Create new value if Today not null
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
	ELSE y.season_stats -- Carry history forward otherwise (retired player)
	END as season_stats,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
SELECT * FROM players WHERE current_season = 2001
-- See specific player, this table is flattened out
SELECT * FROM players 
WHERE current_season = 2001
AND player_name = 'Michael Jordan'
-- See specific player, now we easily convert it to the specific player's player_seasons
SELECT * FROM players
WHERE current_season = 2001
AND player_name = 'Michael Jordan'
-- See specific player, now we easily convert it to the specific player's player_seasons exploding into columns
WITH unnested AS (
	SELECT player_name,
		UNNEST(season_stats)::season_stats AS season_stats
		FROM players
	WHERE current_season = 2001
	AND player_name = 'Michael Jordan'
)
SELECT player_name,
	(season_stats::season_stats).*
FROM unnested

-- Delete
DROP TABLE players;

-- Add Scoring class column, based on the pts columns (cumulative)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'scoring_class'
    ) THEN
        CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

INSERT INTO players
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1995 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1996
	)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE 
		WHEN y.season_stats IS NULL -- Create initial array w/ 1 value if null
			THEN ARRAY[ROW(
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW( -- Create new value if Today not null
							t.season,
							t.gp,
							t.pts,
							t.ast,
							t.reb
							)::season_stats]
	ELSE y.season_stats -- Carry history forward otherwise (retired player)
	END as season_stats,
	CASE 
		WHEN t.season IS NOT NULL THEN -- active this season
		CASE WHEN t.pts > 20 THEN 'star'
			WHEN t.pts > 15 THEN 'good'
			WHEN t.pts > 10 THEN 'average'
			ELSE 'bad'
		END::scoring_class
	END as scoring_class,
	CASE 
		WHEN t.season IS NOT NULL THEN 0 -- active this season (also when they come back)
	ELSE
		y.years_since_last_season + 1 -- otherwise it's been 1 year since they played (incrementing)
	END as years_since_last_season,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name
SELECT * FROM players WHERE current_season = 1996