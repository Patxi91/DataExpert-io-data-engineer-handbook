-----------------------------
--  Proceeding from LAB 1  --
-----------------------------
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
     years_since_last_active INTEGER,
     current_season INTEGER,
	 is_active BOOLEAN,
     PRIMARY KEY (player_name, current_season)
 );
-- select * from players 
insert into players 
with years as (
	select *
	from generate_series(1996, 2022) as season
),
p as (
	select player_name , MIN(season) as first_season 
	from player_seasons 
	group by player_name 
),
players_and_seasons as (
	select * 
	from p
	join years y 
	on p.first_season <= y.season
),
windowed as (
	select 
	ps.player_name, ps.season,
	array_remove(
	array_agg(case 
		when p1.season is not null then 
		cast(row(p1.season, p1.gp, p1.pts, p1.reb, p1.ast) as season_stats)
		end
		)
	over (partition by ps.player_name order by coalesce(p1.season, ps.season)) 
	,null
) 
as seasons
	from players_and_seasons ps
	left join player_seasons p1
	on ps.player_name = p1.player_name and ps.season = p1.season
	order by ps.player_name, ps.season
)
,static as ( 
	select player_name,
	max(height) as height,
	max(college) as college,
	max(country) as country,
	max(draft_year) as draft_year,
	max(draft_round) as draft_round,
	max(draft_number) as draft_number
	from player_seasons ps 
	group by player_name
	)
	
select 
	w.player_name, 
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_number,
	s.draft_round,
	seasons as season_stats
--	,( seasons[cardinality(seasons)]).pts
	,case 
	when (seasons[cardinality(seasons)]).pts > 20 then 'star'
	when (seasons[cardinality(seasons)]).pts > 15 then 'good'
	when (seasons[cardinality(seasons)]).pts > 10 then 'average'
	else 'bad'
	end :: scoring_class as scorring_class
	,w.season - (seasons[cardinality(seasons)]).season as years_since_last_season
	,w.season as current_season
	,(seasons[cardinality(seasons)]).season = w.season as is_active
from windowed w 
join static s
on w.player_name = s.player_name;


-------------------------------------------------
-- LAB 2: Converting Datasets into SCDs Type 2 --
-------------------------------------------------
select 
	player_name,
	scoring_class, 
	is_active
from players
where current_season = 1996
-- SCD Table to model the from-to-the-to year changes (we track multiple columns changes)
CREATE TABLE IF NOT EXISTS players_scd(
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
)
-- We can build 1 SCD from the whole History of Data, then we can use that SCD to cumulative reconstruct the whole History
-- Window Function
select 
	player_name,
	current_season,
	scoring_class,
	is_active,
	LAG(scoring_class, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
	LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
from players
-- CTE with indictors whether the scoring_class and is_active changed
WITH with_previous AS(
	select 
		player_name,
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
		LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
	from players
)
SELECT *,
		CASE
			WHEN scoring_class <> previous_scoring_class THEN 1 
			ELSE 0 
		END AS scoring_class_change_indicator,
		CASE
			WHEN is_active <> previous_is_active THEN 1 
			ELSE 0 
		END AS is_active_change_indicator
FROM with_previous
-- Create a Streak: Identify a change in either active or scoring and sum changes --> SCD Table
WITH
	with_previous AS(
		select 
			player_name,
			current_season,
			scoring_class,
			is_active,
			LAG(scoring_class, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
			LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
		from players
	),
	with_indicators AS( -- combined OR change indicator
		SELECT *,
				CASE
					WHEN scoring_class <> previous_scoring_class THEN 1
					WHEN is_active <> previous_is_active THEN 1 
					ELSE 0 
				END AS change_indicator
		FROM with_previous
	),
	with_streaks AS(-- streak identifier
		SELECT *,
				SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
		FROM with_indicators
	)
SELECT 
	player_name,
	streak_identifier,
	is_active,
	scoring_class,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name
-- Filter down SCD Table to 2021, later to use 2022 in the incremental build
WITH
	with_previous AS(
		SELECT 
			player_name,
			current_season,
			scoring_class,
			is_active,
			LAG(scoring_class, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
			LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
		FROM players
		WHERE current_season <= 2021		
	),
	with_indicators AS( -- combined OR change indicator
		SELECT *,
				CASE
					WHEN scoring_class <> previous_scoring_class THEN 1
					WHEN is_active <> previous_is_active THEN 1 
					ELSE 0 
				END AS change_indicator
		FROM with_previous
	),
	with_streaks AS(-- streak identifier
		SELECT *,
				SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
		FROM with_indicators
	)
SELECT 
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 AS current_season -- hardcoded
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier
-- Modify PKs for players_scd (start_season & player_name) and Insert into the SCD Table
DROP TABLE players_scd
CREATE TABLE IF NOT EXISTS players_scd(
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
)

INSERT INTO players_scd
WITH
	with_previous AS(
		SELECT 
			player_name,
			current_season,
			scoring_class,
			is_active,
			LAG(scoring_class, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
			LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
		FROM players
		WHERE current_season <= 2021		
	),
	with_indicators AS( -- combined OR change indicator
		SELECT *,
				CASE
					WHEN scoring_class <> previous_scoring_class THEN 1
					WHEN is_active <> previous_is_active THEN 1 
					ELSE 0 
				END AS change_indicator
		FROM with_previous
	),
	with_streaks AS(-- streak identifier
		SELECT *,
				SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
		FROM with_indicators
	)
SELECT 
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 AS current_season -- hardcoded
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier

SELECT * FROM players_scd