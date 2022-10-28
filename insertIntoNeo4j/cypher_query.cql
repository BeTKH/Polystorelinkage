//Query relations
MATCH (KS:Person)-[:IS_MARRIED_TO]-(wife), 
(KS:Person)-[:WORKS_AT]-(comp),
(KS:Person)-[:KNOWS]-(friend),

(friend)-[:WORKS_AT]-(comp2)

WHERE KS.Name = "Kevan" 

RETURN KS, wife, comp,comp2, friend;
