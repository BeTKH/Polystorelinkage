import configparser
import json

processCount = 1
entryLimit = 100
mongodb_connection_string = ""
mysql_host = ""
mysql_user = ""
mysql_password = ""
neo4j_uri = ""
neo4j_user = ""
neo4j_password = ""
levMaxLen = 5
typeLists = []
typeDict = {}
initialized = False
synonyms = []
inclusionDpendencyQuota = 1
valueMatchQuota = 1

def init(configDic = {}):
    global initialized
    if initialized:
        return
    if configDic == {}:
        configParser = configparser.RawConfigParser()   
        configFilePath = r'config.txt'
        configParser.read(configFilePath)

        global processCount
        try:
            processCount = configParser.getint('general', 'processCount')
        except:
            print("processCount not defined")
        global entryLimit
        try:
            entryLimit = configParser.getint('general', 'entryLimit')
        except:
            print("entryLimit not defined")
        global inclusionDpendencyQuota
        try:
            inclusionDpendencyQuota = configParser.getfloat('general', 'inclusion.dependency.quota')
        except:
            print("inclusion.dependency.quota not defined")
        global valueMatchQuota
        try:
            valueMatchQuota = configParser.getfloat('general', 'inclusion.dependency.quota')
        except:
            print("inclusion.dependency.quota not defined")
        global mongodb_connection_string
        try:
            mongodb_connection_string = configParser.get('database', 'mongodb.connection.string')
        except:
            print("mongodb_connection_string not defined")
        global mysql_host
        try:
            mysql_host = configParser.get('database', 'mysql.host')
        except:
            print("mysql_host not defined")
        global mysql_user
        try:
            mysql_user = configParser.get('database', 'mysql.user')
        except:
            print("mysql_user not defined")
        global mysql_password
        try:
            mysql_password = configParser.get('database', 'mysql.password')
        except:
            print("mysql_password not defined")
        global neo4j_uri
        try:
            neo4j_uri = configParser.get('database', 'neo4j.uri')
        except:
            print("neo4j_uri not defined")
        global neo4j_user
        try:
            neo4j_user = configParser.get('database', 'neo4j.user')
        except:
            print("neo4j_user not defined")
        global neo4j_password
        try:
            neo4j_password = configParser.get('database', 'neo4j.password')
        except:
            print("neo4j_password not defined")
        global levMaxLen
        try:   
            levMaxLen = configParser.getint('text_comparision', 'levMaxLen')
        except:
            print("levMaxLen not defined")
        global synonyms
        try:
            synonyms = json.loads(configParser.get('text_comparision', 'synonyms'))
        except:
            print("synonyms not defined")
        try:
            initTypeDict(json.loads(configParser.get('types', 'typegroups')))
        except:
            print("typegroups not defined")
    else:
        initFromDic(configDic)
    initialized = True
    
def initFromDic(configDic):
    if "processCount" in configDic:
        global processCount
        processCount = int(configDic["processCount"])
    if "entryLimit" in configDic: 
        global entryLimit
        entryLimit = int(configDic["entryLimit"])
    if "inclusionDpendencyQuota" in configDic: 
        global inclusionDpendencyQuota
        inclusionDpendencyQuota = float(configDic["inclusionDpendencyQuota"])
    if "valueMatchQuota" in configDic: 
        global valueMatchQuota
        valueMatchQuota = float(configDic["valueMatchQuota"])
    if "mongodb_connection_string" in configDic:
        global mongodb_connection_string
        mongodb_connection_string = configDic["mongodb_connection_string"]
    if "mysql_host" in configDic:
        global mysql_host
        mysql_host = configDic["mysql_host"]
    if "mysql_user" in configDic:
        global mysql_user
        mysql_user = configDic["mysql_user"]
    if "mysql_password" in configDic:
        global mysql_password
        mysql_password = configDic["mysql_password"]
    if "neo4j_uri" in configDic:
        global neo4j_uri
        neo4j_uri = configDic["neo4j_uri"]
    if "neo4j_user" in configDic:
        global neo4j_user
        neo4j_user = configDic["neo4j_user"]
    if "neo4j_password" in configDic:
        global neo4j_password
        neo4j_password = configDic["neo4j_password"]
    if "levMaxLen" in configDic:
        global levMaxLen
        levMaxLen = int(configDic["levMaxLen"])
    if "synonyms" in configDic:
        global synonyms
        synonyms = configDic["synonyms"]
    if "typeLists" in configDic:
        global typeLists
        typeLists = configDic["typeLists"]
    if "typeDict" in configDic:
        global typeDict
        typeDict = configDic["typeDict"]

def initTypeDict(typeListsl):
    global typeLists
    typeLists = typeListsl
    global typeDict

    i = 0
    for typeList in typeLists:
        for entry in typeList:
            typeDict.update({entry:i})
        i+=1

def update(configDic):
    if "processCount" in configDic:
        global processCount
        processCount = int(configDic["processCount"])
    if "entryLimit" in configDic: 
        global entryLimit
        entryLimit = int(configDic["entryLimit"])
    if "inclusionDpendencyQuota" in configDic: 
        global inclusionDpendencyQuota
        inclusionDpendencyQuota = float(configDic["inclusionDpendencyQuota"])
    if "valueMatchQuota" in configDic: 
        global valueMatchQuota
        valueMatchQuota = float(configDic["valueMatchQuota"])
    if "mongodb_connection_string" in configDic:
        global mongodb_connection_string
        mongodb_connection_string = configDic["mongodb_connection_string"]
    if "mysql_host" in configDic:
        global mysql_host
        mysql_host = configDic["mysql_host"]
    if "mysql_user" in configDic:
        global mysql_user
        mysql_user = configDic["mysql_user"]
    if "mysql_password" in configDic:
        global mysql_password
        mysql_password = configDic["mysql_password"]
    if "neo4j_uri" in configDic:
        global neo4j_uri
        neo4j_uri = configDic["neo4j_uri"]
    if "neo4j_user" in configDic:
        global neo4j_user
        neo4j_user = configDic["neo4j_user"]
    if "neo4j_password" in configDic:
        global neo4j_password
        neo4j_password = configDic["neo4j_password"]
    if "levMaxLen" in configDic:
        global levMaxLen
        levMaxLen = int(configDic["levMaxLen"])
    if "synonyms" in configDic:
        global synonyms
        synonyms = configDic["synonyms"]
    if "typegroups" in configDic:
        initTypeDict(json.loads(configDic["typegroups"]))

def getConfigAsDict():
    configDict = {}
    global processCount
    configDict.update({"processCount": processCount})
    global entryLimit
    configDict.update({"entryLimit": entryLimit})
    global inclusionDpendencyQuota
    configDict.update({"inclusionDpendencyQuota": inclusionDpendencyQuota})
    global valueMatchQuota
    configDict.update({"valueMatchQuota": valueMatchQuota})
    global mongodb_connection_string
    configDict.update({"mongodb_connection_string": mongodb_connection_string})
    global mysql_host
    configDict.update({"mysql_host": mysql_host})
    global mysql_user
    configDict.update({"mysql_user": mysql_user})
    global mysql_password
    configDict.update({"mysql_user": mysql_password})
    global neo4j_uri
    configDict.update({"neo4j_uri": neo4j_uri})
    global neo4j_user
    configDict.update({"neo4j_user": neo4j_user})
    global neo4j_password
    configDict.update({"neo4j_password": neo4j_password})
    global levMaxLen
    configDict.update({"levMaxLen": levMaxLen})
    global synonyms
    configDict.update({"synonyms": synonyms})
    global typeLists
    configDict.update({"typeLists": typeLists})
    global typeDict
    configDict.update({"typeDict": typeDict})
    return configDict