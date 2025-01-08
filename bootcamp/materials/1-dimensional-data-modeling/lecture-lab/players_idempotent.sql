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
    seasons season_stats[],
    scoring_class scoring_class,
    years_since_last_active INTEGER,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);