class myAttribute:


    def __init__(self, name, dataType, value):
        self.name = name
        self.dataType = str(dataType).split('\'')[1]
        self.values = value
        self.linkedTable = None

    # Wert hinzufügen
    def addValue(self, value):
        if not value in self.values and value != None:
            self.values.append(value)

    def sortValues(self):
        if len(self.values) >0:
            if type(self.values[0]) == str or type(self.values[0]) == int or type(self.values[0]) == float or type(self.values[0]) ==  complex:
                self.values.sort()

    # Tabelle anfügen (bei nested Objects), welche Attribute untergeordnete Attribute und Werte hält
    def attachTable(self, table):
        self.linkedTable = table