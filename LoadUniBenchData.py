import json
import config
import os
import csv
import mysql.connector
import similarity
from neo4j import GraphDatabase

config.init()

filename="Data\\CSV_Customers\\Customer.csv"

def UploadToMySQL(mySQLDB, CsvObject, tablename, types):
    mycursor = mySQLDB.cursor()
    query = "DROP TABLE IF exists "
    query += tablename
    mycursor.execute(query)
    mySQLDB.commit()
    query = "CREATE Table IF NOT exists "
    query += tablename + "("
    
    for i in range(len(CsvObject[0])):
        query += CsvObject[0][i] + " " + types[i] +", "
    query = query.removesuffix(", ")
    query += ')'

    print(query)
    mycursor.execute(query)
    
    query = "INSERT INTO "
    query += tablename + "("
    
    for i in range(len(CsvObject[0])):
        query += CsvObject[0][i] + ", "
    query = query.removesuffix(", ")
    query += ') VALUES ('
    for i in range(len(CsvObject[0])):
        query += '%s, '
    query = query.removesuffix(", ")
    query += ')'
    print(query)
    for val in CsvObject[1]:
        if val[-1] == '':
            val = val[:len(val)-1]
        try:
            mycursor.execute(query, val)
        except:
            print(val)
    mySQLDB.commit()

def uploadToMongoDB(database, collectionName, dataSet):
    database.drop_collection(collectionName)
    for data in dataSet:
        database[collectionName].insert_one(data)

def uploadNodesToNeo4j(neo4jdatabase, label, objects):
    neo4jdatabase.session().run("MATCH (n:"+ label +") DETACH DELETE n")
    for object in objects:
        neo4jdatabase.session().write_transaction(add_Node, label, object)

def uploadEdgesToNeo4j(neo4jdatabase, label, edges, direction):
    i = 1
    for edge in edges[1]:
        print(i/len(edges[1])*100)
        dict1 = {edges[0][0]:edge[0]}
        dict2 = {edges[0][1]:edge[1]}
        neo4jdatabase.session().write_transaction(add_relation, label, dict1, dict2, direction)
        i+=1

def add_Node(tx, label, properties):
    string = ""
    first = True
    for property in properties:
        if not first:
            string += ", "
        if type(properties.get(property)) == str:
            string += property + ": '" + properties.get(property) +"'"
        else:
            string += property + ": " + str(properties.get(property))
        first = False
    tx.run("CREATE (a:" + label + " {" + string + "})")

def add_relation(tx, relType, object1,  object2, direction):
    string = "MATCH (a),(b) WHERE "
    first = True
    for property in object1:
        if not first:
            string += " AND "
        if type(object1.get(property)) == str:
            string += "a." + property + " = '" + object1.get(property) + "'"
        else:
            string += "a." + property + " = " + str(object1.get(property))
        first = False

    for property in object2:
        string += " AND "
        if type(object2.get(property)) == str:
            string += "b." + property + " = '" + object2.get(property) + "'"
        else:
            string += "b." + property + " = " + str(object2.get(property))

    string += " Create (a)"
    if direction == "right":
        string+="-[r:" + relType + "]->(b) RETURN r"

    print(string)
    tx.run(string)

def loadCsvFromGenerator(filename):
    attributes = []
    values = []
    file = open(filename)
    csv_reader=csv.reader(file)
    isAttributeRow = True
    for row in csv_reader:
        if isAttributeRow:
            attributes = row
            isAttributeRow = False
        else:
            values.append(row)
    return(attributes,values)

def loadJsonFromGenerator(directory):
    jsonFiles = []
    for file in os.listdir(directory):
        f = open(directory + '\\' + file)
        jsonFiles.append(json.load(f))
    return jsonFiles
   
def csvToDict(csvObject):
    result = []
    for object in csvObject[1]:
        dictio = {}
        for i in range(len(csvObject[0])):
            dictio.update({csvObject[0][i]:object[i]})
        result.append(dictio)
    return result

def getNeo4JDatabase(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

mydb = mysql.connector.connect(
  host="localhost",
  user="python",
  password="1234",
  database="main_database"
)

orders = loadJsonFromGenerator('Data\JSON_Orders')
products = loadJsonFromGenerator('Data\JSON_Products')
customers = loadCsvFromGenerator('Data\\CSV_Customers\\Customer.csv')
vendors = loadCsvFromGenerator('Data\\CSV_Vendors\\Vendor.csv')
users = csvToDict(loadCsvFromGenerator('Data\\CSV_RegUsers\\RegUsers.csv'))
connections = loadCsvFromGenerator('Data\\Social_Network\\Social_Network.csv')

customerTypes = ("varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "int", "float")
UploadToMySQL(mydb, customers, 'customer', customerTypes)

i = 0
for vendor in vendors[1]:
    if len(vendor) == 5:
        vendor = [vendor[0], vendor[1] + ", " + vendor[2], vendor[3], vendor[4]]
        vendors[1][i] = vendor
    if len(vendor) > 5:
        vendor = [vendor[0], vendor[1] + ", " + vendor[2] + ", " + vendor[3], vendor[4], vendor[5]]
        vendors[1][i] = vendor
    i += 1



vendorsTypes = ("varchar(255)", "varchar(255)", "varchar(255)", "float")
UploadToMySQL(mydb, vendors, 'vendors', vendorsTypes)

mongoDatabase = similarity.get_MongoDatabase("OnlineStore").database

uploadToMongoDB(mongoDatabase, "orders", orders)
uploadToMongoDB(mongoDatabase, "products", products)

neo4jdatabase = getNeo4JDatabase("bolt://localhost:7687", "python", "12345")
for user in users:
    for attribute in user:
        user[attribute] = user[attribute].replace("'","\\'")

uploadNodesToNeo4j(neo4jdatabase, 'user', users)
uploadEdgesToNeo4j(neo4jdatabase, 'Knows', connections, 'right')