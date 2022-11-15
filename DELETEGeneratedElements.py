import mysql.connector
from similarity import get_MongoDatabase
import config
from neo4j import GraphDatabase

config.init()

mydb = mysql.connector.connect(
  host="localhost",
  user="python",
  password="1234",
  database="main_database"    # "main_database" is name of the realtional DB in the mysql 
)

mycursor = mydb.cursor()
mycursor.execute("DROP TABLE if exists articles")
mycursor.execute("DROP TABLE if exists customers")
mydb.commit()

database = get_MongoDatabase("OnlineStore").database

database.drop_collection("accounts")
database.drop_collection("ratings")

def getNeo4JDatabase(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

def closeNeo4JDatabase(database):
    database.close()

neo4jdatabase = getNeo4JDatabase("bolt://localhost:7687", "neo4j", "1234")
neo4jdatabase.session().run("MATCH (n) DETACH DELETE n")
closeNeo4JDatabase(neo4jdatabase)