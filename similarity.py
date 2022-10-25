from myAttribute import myAttribute
import time
import multiprocessing
import numpy as np
import similarityFunctions as sf
import config
from neo4j import GraphDatabase
import mysql.connector
from myDatabase import myDatabase
from myTable import myTable
from pymongo import MongoClient

def getSimilarities(object1, object2, configDic = {}):
    if type(object1) == myDatabase and type(object2) == myDatabase:
        return getDatabaseSimilarities(object1, object2, configDic)
    elif type(object1) == myDatabase:
        return getTableDatabaseSimilarity(object2, object1, configDic)
    elif type(object2) == myDatabase:
        return getTableDatabaseSimilarity(object1, object2, configDic)
    elif type(object1) == myTable and type(object2) == myTable:
        return getTableSimilarities(object1, object2, configDic)
    else:
        print("only myTable, myDatabase types allowed")

def getTableSimilarities(table1, table2, configDic = {}):
    database1 = myDatabase(table1.name, None, "table")
    database1.tables = [table1]
    database2 = myDatabase(table2.name, None, "table")
    database2.tables = [table2]
    return getDatabaseSimilarities(database1, database2, configDic, False)

def getTableDatabaseSimilarity(table1, database1, configDic = {}):
    database2 = myDatabase(table1.name, None, "table")
    database2.tables = [table1]
    return getDatabaseSimilarities(database1, database2, configDic, False)

# sucht nach Überschneidungen innerhalb von 2 Datenbanken
# searches for overlaps within 2 databases
def getDatabaseSimilarities(database1, database2, configDic = {}, loadTables = True):
    start = time.time()
    config.init()
    config.update(configDic)
    if loadTables:
        database1.loadTables()
        database2.loadTables()
    
    #Inclusiondependencies zwischen allen Tabellen finden
    #Find inclusiondependencies between all tables
    inclusionDependencies = {}

    for table1 in database1.tables:
        table1Inclusiondependencies = {}
        for table2 in database2.tables:
            table1Inclusiondependencies.update({table2.name:getInclusiondependencies(table1, table2)})
        inclusionDependencies.update({table1.name:table1Inclusiondependencies})

    print("-------------")
    getModelInclusiondependencies(database1, database2)

    #init a matrix that compares every row of the databases with 1s
    #Initialisierung einer Matrix bei der die Gleichhalt aller Attribute aufeinander festgehalten wird Struktur: Tabelle 1 von Datenbank 1->Tabelle 2 von Datenbank2->Attribut von Tabelle 1 -> Attribut von Tabelle 2
    #Initialization of a matrix in which the equality of all attributes is recorded Structure: Table 1 of database 1 -> table 2 of database 2 -> attribute of table 1 -> attribute of table 2
    typeSimilarityMatrix=initSimilarityMatrix(database1,database2)


    #compare types
    #Typen werden verglichen und Ergebnis in Matrix festgehalten
    #Types are compared and the result is recorded in the matrix
    for table1 in database1.tables:
        for table2 in database2.tables:
            compareAttributesTypes(table1, table2, typeSimilarityMatrix)
    
    printMapping(typeSimilarityMatrix)
    print("-------------")

    nameSimilarityMatrix=initSimilarityMatrix(database1,database2)
    #compare attributnames
    #Vergleich der Namen von Attributen. Ähnlichkeit im Intervall von [0,1]. Wird mit Matrix multipliziert.
    #Comparison of the names of attributes. Similarity in the interval of [0,1]. Is multiplied by matrix.
    for table1 in database1.tables:
        for table2 in database2.tables:
            compareAttributesNames(table1, table2, nameSimilarityMatrix)

    printMapping(nameSimilarityMatrix)
    print("-------------")
    
    valueSimilarityMatrix = initSimilarityMatrix(database1,database2)
    #compare AttributeValues
    #Werte werden Verglichen. Ähnlichkeit im Intervall [0,1] Wird mit Matrix multipliziert.
    #Values ​​are compared. Similarity in interval [0,1] Multiplied by matrix.
    for table1 in database1.tables:
        for table2 in database2.tables:
            compareAttributeValues(table1, table2, valueSimilarityMatrix)
    
    finalSimilarityMatrix = getSimilarityMatrix(database1, database2, typeSimilarityMatrix, nameSimilarityMatrix, valueSimilarityMatrix)
    #updateContext(finalSimilarityMatrix, )

    #print(getInterModelLinkage(database1, database2, finalSimilarityMatrix))
    #updateContext(finalSimilarityMatrix, inclusionDependencies)
    end = time.time()
    print(end - start)
    printMapping(finalSimilarityMatrix)
    print('_______________________________________________________________')
    print("last name")
    print('_______________________________________________________________')
    print(getInterModelLinkage(database1, database2, nameSimilarityMatrix))
    print('_______________________________________________________________')
    print("by value")
    print('_______________________________________________________________')
    print(getInterModelLinkage(database1, database2, valueSimilarityMatrix))
    print('_______________________________________________________________')
    print("according to a strange formula")
    print('_______________________________________________________________')
    print(getInterModelLinkage(database1, database2, finalSimilarityMatrix))
    print('_______________________________________________________________')
    print("inclusion dependencies")
    print('_______________________________________________________________')
    print(inclusionDependencies)
    print('_______________________________________________________________')
    return (getInterModelLinkage(database1, database2, finalSimilarityMatrix),inclusionDependencies)

#returns matrix(dicts) that compares all attributes of both collections with 1s
# initialisiert für 2 Datenbanken eine Matrix, die die Ähnlichkeiten der beiden beschreibt mit Werten im Intervall [0,1] und initialisiert sie mit 1
# Struktur ist ein dic mit similarityMatrix[Tabelle 1 von Datenbank 1][Tabelle 2 von Datenbank 2][Attribut von Tabelle 1][Attribut von Tabelle 2]
# initializes a matrix for 2 databases that describes the similarities between the two with values ​​in the interval [0,1] and initializes them with 1
# struct is a dic with similarityMatrix[table 1 of database 1][table 2 of database 2][attribute of table 1][attribute of table 2]
def initSimilarityMatrix(database1,database2):
    similarityMatrix= {}
    for table1 in database1.tables:
        table1dic = {}
        for table2 in database2.tables:
            table2dic = {}
            for attribute1 in table1.attributes:
                attribute1dic ={}
                for attribute2 in table2.attributes:
                    attribute1dic.update({attribute2.name:float(1)})
                table2dic.update({attribute1.name:attribute1dic})
            table1dic.update({table2.name:table2dic})
        similarityMatrix.update({table1.name:table1dic})

    return similarityMatrix

#compares types of 2 collections and updates similarityMatrix
#vergleicht die Typen von 2 Tabellen und trägt 1 in die Matrix ein bei gleicher Äquivalenzklasse, 0 sonst
#compares the types of 2 tables and enters 1 in the matrix if the equivalence class is the same, 0 otherwise
def compareAttributesTypes(table1, table2, similarityMatrix):
    for attribute1 in table1.attributes:
        for attribute2 in table2.attributes:
            similar = 0
            if sf.compareTypes(attribute1.dataType, attribute2.dataType):
                similar = 1
            similarityMatrix[table1.name][table2.name][attribute1.name].update({attribute2.name: float(similar)})

#compares property names of 2 collections and updates similarityMatrix
#Vergleicht die Namen der Attribute von 2 Tabellen. Ähnlichkeit liegt zwischen 0 und 1.
#Compares the names of the attributes of 2 tables. Similarity is between 0 and 1.
def compareAttributesNames(table1, table2, similarityMatrix):
    updatableAtributes=[]
    for attribute1 in table1.attributes:
        for attribute2 in table2.attributes:
            if similarityMatrix[table1.name][table2.name][attribute1.name][attribute2.name] >0:
                updatableAtributes.append((attribute1.name,attribute2.name))

    p = multiprocessing.Pool(processes=config.processCount)
    result = p.starmap(sf.getExtendedWordSimilarity, updatableAtributes)
    for value in updatableAtributes:
        sf.getExtendedWordSimilarity(value[0], value[1])
    for i in range(len(result)):
        similarityMatrix[table1.name][table2.name][updatableAtributes[i][0]].update({updatableAtributes[i][1]:result[i]})

#compares values of 2 Collection and updates similarityMatrix
#vergleicht die Werte der Attribute von 2 Tabellen und multipliziert Ergebnis im Bereich von [0,1] mit Eintrag in Matrix
#compares the values ​​of the attributes of 2 tables and multiplies the result in the range of [0,1] by the entry in the matrix
def compareAttributeValues(table1, table2, similarityMatrix):

    toProcessList = []
    toProcessNames = []

    for attribute1 in table1.attributes:
        for attribute2 in table2.attributes:
            if sf.compareTypes(attribute1.dataType, attribute2.dataType):
                toProcessList.append((attribute1.values, attribute2.values))
                toProcessNames.append((attribute1.name, attribute2.name))
            else:
                similarityMatrix[table1.name][table2.name][attribute1.name].update({attribute2.name:0})
         
    
    p = multiprocessing.Pool(processes=config.processCount)
    result = p.starmap(sf.compareLists, toProcessList)
    #for element in toProcessList:
    #    sf.compareLists(element[0],element[1])
    for i in range(len(result)):
        similarityMatrix[table1.name][table2.name][toProcessNames[i][0]].update({toProcessNames[i][1]:similarityMatrix[table1.name][table2.name][toProcessNames[i][0]][toProcessNames[i][1]]*result[i]})

#ermittelt inclusion dependencies zwischen 2 Tabellen [[tabelle 1 ⊆ tablle 2],[tabelle 1 ⊇ tabelle 2]]
# determines inclusion dependencies between 2 tables [[table 1 ⊆ table 2],[table 1 ⊇ table 2]]
def getInclusiondependencies(table1, table2):
    result = [[],[]]
    table1Inclusions = {}
    for entity1 in table1.entities:
        entity1Inclusions = []
        for entity2 in table2.entities:
            entityInclusion = getEntityInclusion(entity1, entity2)    
            if len(entityInclusion)>0:
                entity1Inclusions.append(getAllInclusions(entityInclusion))
        entity1Inclusions = removeDuplicates(entity1Inclusions)
        for inclusions in entity1Inclusions:
            for inclusion in inclusions:
                if inclusion in table1Inclusions:
                    table1Inclusions[inclusion] += 1
                else:
                    table1Inclusions.update({inclusion:1})
    print(table1Inclusions)
    for inclusion in table1Inclusions:
        if table1Inclusions[inclusion] / len(table1.entities) >= config.inclusionDpendencyQuota:
            result[0].append(inclusion)

    table2Inclusions = {}
    for entity2 in table2.entities:
        entity2Inclusions = []
        for entity1 in table1.entities:
            entityInclusion = getEntityInclusion(entity2, entity1)    
            if len(entityInclusion)>0:
                entity2Inclusions.append(getAllInclusions(entityInclusion))
        entity2Inclusions = removeDuplicates(entity2Inclusions)
        for inclusions in entity2Inclusions:
            for inclusion in inclusions:
                if inclusion in table2Inclusions:
                    table2Inclusions[inclusion] += 1
                else:
                    table2Inclusions.update({inclusion:1})
    print(table2Inclusions)
    for inclusion in table2Inclusions:
        if table2Inclusions[inclusion] / len(table2.entities) >= config.inclusionDpendencyQuota:
            result[1].append(inclusion)

    return result

# generiert aus einelementigen inclusiondependencies alle k-elementigen
# generates all k-element from single-element inclusiondependencies
def getAllInclusions(inclusions):
    result=[]
    head = inclusions[0]
    result.append(head)
    inclusions.pop(0)
    if len(inclusions)>0:
        tail = getAllInclusions(inclusions)
        result += tail
        for value in tail:
            result.append(((value[0] + head[0]),(value[1] + head[1])))

        
    return result

#entfernt Duplikate aus einer Liste
#removes duplicates from a list
def removeDuplicates(list):
    result = []
    for element in list:
        if result.count(element) <= 0:
            result.append(element)
    return result

#gibt alle einelementigen inclusion dependencies zwischen 2 Entitäten zurück
#returns all single-element inclusion dependencies between 2 entities
def getEntityInclusion(entity1, entity2):
    result=[]
    for value1 in entity1:
        for value2 in entity2:
            if sf.compareValues(entity1[value1], entity2[value2]) >= 1:
                #print(entity1[value1])
                result.append(((value1,),(value2,)))
    return result

def getModelInclusiondependencies(database1, database2):
    result = []
    database1Inclusions = {}
    for table1 in database1.tables:
        table1Inclusions = {}
        for table2 in database2.tables:
            tableInclusion = getTableInclusion(table1, table2)    
            if len(tableInclusion)>0:
                table1Inclusions.update({table2.name:getAllInclusions(tableInclusion)})
        database1Inclusions.update({table1.name:table1Inclusions})
    print(database1Inclusions)
    result.append(database1Inclusions)

    database2Inclusions = {}
    for table2 in database2.tables:
        table2Inclusions = {}
        for table1 in database1.tables:
            tableInclusion = getTableInclusion(table2, table1)    
            if len(tableInclusion)>0:
                table2Inclusions.update({table1.name:getAllInclusions(tableInclusion)})
        database2Inclusions.update({table2.name:table2Inclusions})

    print(database2Inclusions)
    result.append(database2Inclusions)

    return result

def getTableInclusion(table1, table2):
    result=[]
    for attribute1 in table1.attributes:
        for attribute2 in table2.attributes:
            if sf.compareValues(attribute1.name, attribute2.name) >= 1:
                result.append(((attribute1.name,),(attribute2.name,)))
    return result

#compares entities of 2 collections
def compareCollectionEntities(table1, table2):
    table1.loadEntities()
    table2.loadEntities()

    table1Entities = table1.entities
    table2Entities = table2.entities


    entity1Sum = 0;
    for entity in table1Entities:
        entity1Sum += entityInclusion(entity, table2Entities)

    entity2Sum = 0;
    for entity in table2Entities:
        entity2Sum += entityInclusion(entity, table1Entities)
    return (entity2Sum/len(table2Entities),entity1Sum/len(table1Entities))

# deprecated
#returns inclusionrate from Matrix
def getInclusion(inclusionMatrix):
    return max(np.mean(np.amax(inclusionMatrix,1)),np.mean(np.amax(inclusionMatrix,0)))

#prints Mapping from similarityMarix
def printMapping(similarityMatrix):
    for collection1 in similarityMatrix:
        print('\n' + collection1)
        for collection2 in similarityMatrix[collection1]:
            print(' - ' + collection2)
            for attribute1 in similarityMatrix[collection1][collection2]:
                for attribute2 in similarityMatrix[collection1][collection2][attribute1]:
                    value = similarityMatrix[collection1][collection2][attribute1][attribute2]
                    if value>0.5 :
                        print(attribute1 + " -> " + attribute2 + " | " + str(value))




#connects to database and creates database object of type mysql
def get_MySQLDatabase(dbName):
    config.init()
    mySQLDatabase = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=dbName
        )
    database = myDatabase(dbName, mySQLDatabase)
    return database

#connects to database and creates database object of type mongodb
def get_MongoDatabase(dbName):
    config.init()
    client = MongoClient(config.mongodb_connection_string)

    database = myDatabase(dbName, client[dbName])
    database.setMongoClient(client)
    return database

def getNeo4JDatabase(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

#stellt Verbindung zur Datenbank her und erstellt Datenbankobjekt vom Typ neo4j
#connects to the database and creates a database object of type neo4j
def get_Neo4jDatabase(dbName):
    config.init()
    neo4jdatabase = getNeo4JDatabase(config.neo4j_uri, config.neo4j_user, config.neo4j_password)
    database = myDatabase(dbName, neo4jdatabase)
    return database

def getSimilarityMatrix(database1, database2, typeSimilarityMatrix, nameSimilarityMatrix, valueSimilarityMatrix):
    similarityMatrix= {}
    for table1 in database1.tables:
        table1dic = {}
        for table2 in database2.tables:
            table2dic = {}
            for attribute1 in table1.attributes:
                attribute1dic ={}
                for attribute2 in table2.attributes:
                    typeSimilarity = typeSimilarityMatrix[table1.name][table2.name][attribute1.name][attribute2.name]
                    nameSimilarity = nameSimilarityMatrix[table1.name][table2.name][attribute1.name][attribute2.name]
                    valueSimilarity = valueSimilarityMatrix[table1.name][table2.name][attribute1.name][attribute2.name]
                    attribute1dic.update({attribute2.name:typeSimilarity*max(nameSimilarity,valueSimilarity)})
                table2dic.update({attribute1.name:attribute1dic})
            table1dic.update({table2.name:table2dic})
        similarityMatrix.update({table1.name:table1dic})

    return similarityMatrix

def updateContext(similarityMatrix, inclusionDependencies):
    for table1 in inclusionDependencies:
        for table2 in inclusionDependencies[table1]:
            table1Table2Inclusion = inclusionDependencies[table1][table2][0]
            table2Table1Inclusion = inclusionDependencies[table1][table2][1]
            print(table1Table2Inclusion)

def getInterModelLinkage(database1, database2, similarityMatrix):
    InterModelLinkage = {}
    for table1 in database1.tables:
        table1dic = {}
        for table2 in database2.tables:
            inclusions = []
            for attribute1 in table1.attributes:
                for attribute2 in table2.attributes:
                    if similarityMatrix[table1.name][table2.name][attribute1.name][attribute2.name] >= config.inclusionDpendencyQuota:
                        inclusions.append((attribute1.name,attribute2.name))
            table1dic.update({table2.name:inclusions})
        InterModelLinkage.update({table1.name:table1dic})

    return InterModelLinkage