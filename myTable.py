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
    #linked table to show relationship (unused) e.g. At the graph or nested
    def linkToTable(self, table):
        self.linkedTables.update({table.name:table})

    #Entitäten als dic setzten
    #Set entities as dic
    def setEntities(self, values):
        self.entities = values

    # fügt Entität hinzu
    # adds entity
    def addEntity(self, entity):
        self.entities.append(entity)

    # fügt Attribut hinzu
    # add attribute
    def addAttribute(self, attribute):
        if not attribute in self.attributes:
            self.attributes.append(attribute)
    
    # Attribute setzen
    # add attributes
    def addAttributes(self, attributes):
        for attribute in attributes:
            self.addAttribute(attribute)