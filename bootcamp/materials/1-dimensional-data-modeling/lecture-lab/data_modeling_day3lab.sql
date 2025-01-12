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
