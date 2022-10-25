import similarity
import getopt, sys

def group(list):
    groupList = []
    i = 0
    while i+1 < len(list):
        groupList.append((list[i],list[i+1]))
        i = i+2
    return groupList

if __name__ == "__main__":
    configDic = {}
    argumentList = sys.argv[1:]
    groupList = group(argumentList)   # collection of special variables 

    for element in groupList:
        configDic.update({element[0]:element[1]})
    
    
    db1 = similarity.get_Neo4jDatabase("neo4j")
    db2 = similarity.get_MongoDatabase("OnlineStore")
    db3 = similarity.get_MySQLDatabase("mysql")
    
   
    print(configDic)
    
    #db.loadTables()
    #db2.loadTables()
    #configDic.update({"levMaxLen": 2})
    #similarity.getSimilarities(db.tables[1],db)
    #print(similarity.getDatabaseSimilarities(db1, db2, configDic))
    print(similarity.getDatabaseSimilarities(db1, db3, configDic))

    
    
    db1.close()
    db2.close()
    db3.close()