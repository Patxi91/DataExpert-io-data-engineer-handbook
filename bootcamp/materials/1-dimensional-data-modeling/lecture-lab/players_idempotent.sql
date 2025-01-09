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
-- Load Year by Year 1998
INSERT INTO players
WITH yesterday AS(
	SELECT * FROM players
	WHERE current_season = 1998 -- Since SELECT MIN(season) FROM public.player_seasons = 1996
),
	today AS (
		SELECT * FROM public.player_seasons
		WHERE season = 1999
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
SELECT * FROM players WHERE current_season = 1999
