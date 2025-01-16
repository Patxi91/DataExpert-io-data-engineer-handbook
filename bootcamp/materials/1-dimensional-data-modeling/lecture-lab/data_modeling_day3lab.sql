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

-- Vertices and Edges
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
