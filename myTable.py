import mysql.connector
from pymongo import MongoClient

class myTable:


    def __init__(self, name, attributes, database):
        self.name = name
        self.attributes = attributes
        self.database = database
        self.entities = []
        self.linkedTables = {}

    #verknüpft Tabelle um Beziehung darzustellen (Ungenutzt) zB. Beim Graph oder nested
    def linkToTable(self, table):
        self.linkedTables.update({table.name:table})

    #Entitäten als dic setzten
    def setEntities(self, values):
        self.entities = values

    # fügt Entität hinzu
    def addEntity(self, entity):
        self.entities.append(entity)

    # fügt Attribut hinzu
    def addAttribute(self, attribute):
        if not attribute in self.attributes:
            self.attributes.append(attribute)
    
    # Attribute setzen
    def addAttributes(self, attributes):
        for attribute in attributes:
            self.addAttribute(attribute)