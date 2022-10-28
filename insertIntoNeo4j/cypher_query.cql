
MATCH (ks:person)-[:IS_MARRIED_TO]-(wife), 
(ks:person)-[:WORKS_AT]-(comp),
(ks:person)-[:FREIND]-(friend),
(friend)-[:WORKED_AT]-(comp2),
(friend)-[:STUDIED_AT]-(uni),
(wife)-[:KNOWS]-(friend), 
(friend)-[:LIVED]-(city)

WHERE ks.name = "Kevin"

RETURN ks, wife, comp, comp2, friend, uni, city;