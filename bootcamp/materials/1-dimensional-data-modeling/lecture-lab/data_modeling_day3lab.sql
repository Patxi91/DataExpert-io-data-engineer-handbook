-------------------------------------------------
--   LAB 3: Graph DBs & Additive Dimensions    --
-------------------------------------------------
/*
Enumerations and Subpartitions pattern usecases:
- Whenever you have tons of sources mapping to a shared schema/logic/output
    - Unit Economics (fees, coupons, credits, insurance, infrastructure cost, taxes, etc)
    - Infrastructure Graph (applications, DBs, servers, code bases, CI/CD jobs, etc)
    - Family of Apps (oculus, instagram, facebook, messenger, whastapp, threads, etc)
- How:
    - Flexible schema leveraging the map datatype: No ALTER TABLE, add key to map when an extra column is needed (~65k limit)
- Graph Modeling:
    - Usually the model takes 2 Nodes and links them through an edge, this looks like:
        - subject_identifier: STRING
        - subject_type: VERTEX_TYPE
        - object_identifier: STRING
        - object_type: VERTEX_TYPE
        - edge_type: EDGE_TYPE
        - properties: MAP<STRING,STRING>
    - Example: "Player plays on team"
        - subject_identifier: player_name
        - subject_type: player
        - object_identifier: team_name
        - object_type: team
        - edge_type: PLAYS_ON
        - properties: How many years did they play on that team, when did they start, etc.
- Graph example: [ChicagoBulls]<--Plays_ON-->[MichaelJordan]<--Plays_AGAINST-->[JohnStockton]<--Plays_ON-->[UtahJazz]
*/

------------------------
--  Vertices & Edges  --
------------------------
DO $$
BEGIN
    -- Check and drop vertex_type if it exists
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vertex_type') THEN
        DROP TYPE vertex_type CASCADE;
    END IF;

    -- Check and drop edge_type if it exists
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'edge_type') THEN
        DROP TYPE edge_type CASCADE;
    END IF;
END $$;

DROP TABLE IF EXISTS vertices;
DROP TABLE IF EXISTS edges;


-- Objects
CREATE TYPE vertex_type
	AS ENUM('player', 'team', 'game');

CREATE TABLE vertices(
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY(identifier, type)
);

-- Relationships
CREATE TYPE edge_type AS ENUM(
	'plays_against',
	'shares_team',
	'plays_in',
	'plays_on');

CREATE TABLE edges(
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (	subject_identifier,
					subject_type,
					object_identifier,
					object_type,
					edge_type)
);

----------------
--  Vertices  --
----------------

-- Game
-- Let's think of game as a vertex
INSERT INTO vertices
SELECT
	game_id AS identifier,
	'game'::vertex_type AS type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) as properties
FROM games;

-- Player
-- Player details will be an aggregated view from table game_details since we have player_name available
INSERT INTO vertices
WITH players_agg AS(
	SELECT
		player_id AS identifier,
		MAX(player_name) AS player_name,
		COUNT(1) as number_of_games,
		SUM(pts) as total_points,
		ARRAY_AGG(DISTINCT team_id) AS teams
	FROM game_details
	GROUP BY player_id
)
SELECT 
	identifier,
	'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
		)
FROM players_agg

-- Team
INSERT INTO vertices
WITH teams_deduped AS( -- dedupe as for some reason data was triplicated
	SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
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
		)
FROM teams_deduped
WHERE row_num = 1;

-- Query Vertices
SELECT type, COUNT(1)
FROM vertices
GROUP BY 1
/*
| type    | count |
|---------|-------|
| team    | 30    |
| game    | 9384  |
| player  | 1496  |
*/

-------------
--  Edges  --
-------------

-- plays_in
INSERT INTO edges
WITH game_details_deduped AS (
	SELECT *, row_number() over(PARTITION BY player_id, game_id) AS row_num
	FROM game_details
)
SELECT
	player_id AS subject_identifier,
	'player'::vertex_type as subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS object_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
		) as properties
FROM game_details_deduped
WHERE row_num = 1;

select
    v.properties->>'player_name',
    max(cast(e.properties->>'pts' as integer))
from vertices v
    join edges e
on e.subject_identifier = v.identifier
and e.subject_type = v.type
group by 1
order by 2 desc;

insert into edges
with deduped as (
    select *, row_number() over (partition by player_id, game_id) as row_num
    from game_details
),
    filtered as (
        select * from deduped
                 where row_num = 1
    ),
    aggregated as (
        select
            f1.player_id as subject_player_id,
            f2.player_id as object_player_id,
            case when f1.team_abbreviation = f2.team_abbreviation
                then 'shares_team'::edge_type
                else 'plays_against'::edge_type
            end as edge_type,
            max(f1.player_name) as subject_player_name, -- maybe they changed their name
            max(f2.player_name) as object_player_name,
            count(1) as num_games,
            sum(f1.pts) as subject_points,
            sum(f2.pts) as object_points
        from filtered f1 join filtered f2
        on f1.game_id = f2.game_id
        and f1.player_name <> f2.player_name
        where f1.player_name > f2.player_name -- remove double edges
        group by f1.player_id,
            f2.player_id,
            case when f1.team_abbreviation = f2.team_abbreviation
                then 'shares_team'::edge_type
                else 'plays_against'::edge_type
            end
    )
select
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type as edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    )
from aggregated;

-- we can calculate avg points, points when X plays with Y
-- or points when X plays vs Y, etc.
select
    v.properties->>'player_name',
    e.object_identifier,
    cast(v.properties->>'number_of_games' as real) /
    case when cast(v.properties->>'total_points' as real) = 0 then 1
        else cast(v.properties->>'total_points' as real) end,
    e.properties->>'subject_points',
    e.properties->>'num_games'

from vertices v join edges e
    on v.identifier = e.subject_identifier
    and v.type = e.subject_type
where e.object_type = 'player'::vertex_type
