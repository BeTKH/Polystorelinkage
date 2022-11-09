from neo4j import GraphDatabase

def getNeo4JDatabase(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

def closeNeo4JDatabase(database):
    database.close()

def add_Node(database, label, properties):
    string = ""
    for property in properties:
        string += property + ": \'" + properties.get(property) +"\'"
    database.session().run("CREATE (a:" + label + " {" + string + "})")

def add_relation(database, relType, object1,  object2, direction):
    string = "MATCH (a),(b) WHERE "
    first = True
    for property in object1:
        if not first:
            string += " AND "
        if type(object1.get(property)) == str:
            string += "a." + property + " = '" + object1.get(property) + "'"
        else:
            string += "a." + property + " = " + object1.get(property)
        first = False

    for property in object2:
        string += " AND "
        if type(object2.get(property)) == str:
            string += "b." + property + " = '" + object2.get(property) + "'"
        else:
            string += "b." + property + " = " + object2.get(property)

    string += " Create (a)"
    if direction == "right":
        string+="-[r:" + relType + "]->(b) RETURN r"

    print(string)
    database.session().run(string)


    #MATCH (a:Person),(b:Person)
    #WHERE a.name = 'A' AND b.name = 'B'
    #CREATE (a)-[r:RELTYPE]->(b)
        
def create_person_node(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)

if __name__ == "__main__":
    database = getNeo4JDatabase("bolt://localhost:7687", "neo4j", "1234")
    andy = {}
    andy.update({"firstname":"Andy"})
    andy.update({"surname":"Bandy"})
    horst = {}
    horst.update({"firstname":"Horst"})
    horst.update({"surname":"Neumann"})
    add_relation(database, "mag", horst, andy, "right")
    closeNeo4JDatabase(database)