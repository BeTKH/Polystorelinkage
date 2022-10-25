import mysql.connector
import pymongo.database
import neo4j
from myTable import myTable
from myAttribute import myAttribute
import config

class myDatabase:

    def __init__(self, name, database, dbType = ""):
        if dbType == "":
            if type(database) == mysql.connector.connection_cext.CMySQLConnection:
                dbType = "mysql"
            if type(database) == pymongo.database.Database:
                dbType = "mongodb"
            if type(database) == neo4j.BoltDriver:
                dbType = "neo4j"
        self.name = name
        self.dbType = dbType
        self.database = database
        self.tables = None

    def setMongoClient(self, client):
        self.client = client

    # ruft jeweilige Funktion zum Laden der Tabellen des Datenbanktyps auf
    # calls respective function for loading tables of database type
    def loadTables(self):
        if self.dbType == "mysql":
            self.tables = self.loadMySQLTables()
            return

        if self.dbType == "mongodb":
            self.tables = self.newLoadMongoDBTables()
            return

        if self.dbType == "neo4j":
            self.tables = self.loadNeo4jTables()
            return

        print("unsopported database: " + self.dbType)

    # Lädt Tabellen für mySQL
    # Load tables for mySQL
    def loadMySQLTables(self):
        cursor = self.database.cursor()
        cursor.execute("show tables")
        tables = []
        sql=("show columns from ")
        for table in cursor.fetchall():
            cursor.execute(sql + table[0])
            attributes = []
            for column in cursor.fetchall():
                attributes.append(myAttribute(column[0], column[1], []))
            cursor.execute("Select * From " + table[0] + " LIMIT " + str(config.entryLimit))
            entities = cursor.fetchall()
            tableEntities = []
            for entity in entities:
                entitydic = {}
                for i in range(len(entity)):
                    attributes[i].addValue(entity[i])
                    entitydic.update({attributes[i].name:entity[i]})
                tableEntities.append(entitydic)
            for attribute in attributes:
                attribute.sortValues()
            newTable = myTable(table[0], attributes, self)
            newTable.setEntities(tableEntities)
            tables.append(newTable)
        return tables

    # Lädt Tabellen für mongoDB aus Collections
    # Load tables for mongoDB from Collections
    def newLoadMongoDBTables(self):
        dbCollectionNames = self.database.list_collection_names()
        collections = []
        for name in dbCollectionNames:
            collection = self.database[name]
            documents = list(collection.find().limit(config.entryLimit))
            attributes, tables = self.mongoLoadAttributes(documents)
            newMyTable = myTable(name, attributes, self)
            newMyTable.setEntities(documents)
            collections.append(newMyTable)
            for table in tables:
                table.linkToTable(newMyTable)
                for attribute in newMyTable.attributes:
                    if table.name == attribute.name:
                        attribute.attachTable(table)
            collections += tables
        return collections

    #lädt Attribute aus Liste von MongoDB Dokumenten
    #loads attributes from list of MongoDB documents
    def mongoLoadAttributes(self, documents):
        attributes = {}
        objects = {}
        for document in documents:
            for key in document.keys():
                if type(document[key]) == dict:
                    if key not in objects.keys():
                        objects.update({key:[]})
                    objects[key].append(document[key])
                if key not in attributes.keys():
                    attributes.update({key:myAttribute(key, type(document[key]), [])})
                attributes[key].addValue(document[key])
        tables = []
        for key in objects.keys():
            newAttributes,newTables = self.mongoLoadAttributes(objects[key])
            newMyTable = myTable(key, newAttributes, None)
            newMyTable.setEntities(objects[key])
            tables.append(newMyTable)
            for table in newTables:
                table.linkToTable(newMyTable)
            tables += newTables

        for attribute in attributes:
            attributes[attribute].sortValues()
        return list(attributes.values()), tables

    #lädt Tabellen aus neo4j Knoten unter gleichem Label
    #loads tables from neo4j nodes under the same label
    def loadNeo4jTables(self):
        labels = self.database.session().run("CALL db.labels()").value()
        tables = {}
        for label in labels:
            tables.update({label:myTable(label, [], None)})

        for label in labels:
            attributes = {}
            nodes = self.database.session().run("MATCH (n:" + label + ") RETURN n LIMIT " + str(config.entryLimit)).value()
            nodeDics = []
            for node in nodes:
                nodeDic = node.__dict__.get('_properties');
                nodeDics.append(nodeDic)
                keys = node.keys()
                for key in keys:
                    if not key in attributes.keys():
                        attributes.update({key: myAttribute(key,type(node.get(key)), [])})
                    attributes[key].addValue(node.get(key))
                relatedNodes = self.getNeo4jConnectedNodes(node)
                for relatedNode in relatedNodes:
                    for relatedLabel in relatedNode.labels:
                        if not relatedLabel in attributes.keys():
                            newAttribute = myAttribute(relatedLabel,type(relatedNode), [])
                            attributes.update({relatedLabel: newAttribute})
                            newAttribute.attachTable(tables[relatedLabel])
                            tables[relatedLabel].linkToTable(tables[label])
                        attributes[relatedLabel].addValue(relatedNode)
            tables[label].addAttributes(list(attributes.values()))
            for attribute in tables[label].attributes:
                attribute.sortValues()
            tables[label].setEntities(nodeDics)
            

        return list(tables.values())

    # ermittelt verbundene Knoten mit Beziehung node-->neighbournode
    # find connected nodes with relation node-->neighbournode
    def getNeo4jConnectedNodes(self, node):
        string = "MATCH (n)-->(m) Where "
        items = node.items()
        first = True
        for item in items:
            if not first:
                string += " AND "
            if type(item[1]) == str:
                string += "n." + item[0] + " = '" + item[1] + "'"
            else:
                string += "n." + item[0] + " = " + str(item[1])
            first = False
        string += " RETURN m"
        nodes = self.database.session().run(string).value()
        return nodes
            
    def close(self):
        if self.dbType == "mysql" or self.dbType == "neo4j":
            self.database.close()
        if self.dbType == "mongodb":
            self.client.close()