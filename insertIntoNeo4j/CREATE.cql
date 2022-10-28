// create a graph
CREATE (KS:Person {Name : "Kevan", lName : "Smith"}), 
(PS:Person {Name : "Priscilla", lName : "Smith"}), 
(BT:Person {Name : "Bekalu", lName : "Tadesse"}),
(HL:Organization {name: "Hope Lutheran Church"}),
(UR:Organization {name: "UNiversity of Regensburg"}),
(KS)-[:IS_MARRIED_TO {Since:1960}]->(PS), 
(KS)-[:KNOWS {Since:2013}]->(BT),
(KS)-[:WORKS_AT]->(HL),
(BT)-[:WORKS_AT]->(UR)