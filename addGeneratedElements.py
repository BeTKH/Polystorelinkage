from similarity import get_MongoDatabase
import random
import datetime
from person import person
from account import account
from product import product
import mysql.connector
import config
from neo4j import GraphDatabase
import json


config.init()


def loadJsonFromGenerator(directory):
    f = open(directory)
    return json.load(f)['objects']

peoples = loadJsonFromGenerator('People.json')
movies = loadJsonFromGenerator('Movies.json')
books = loadJsonFromGenerator('Books.json')
music = loadJsonFromGenerator('Music.json')

def getNeo4JDatabase(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

def closeNeo4JDatabase(database):
    database.close()

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

products = ["car", "chocolate bar", "monitor", "jet", "rocket", "airplane", "Album", "keyboard", "apple", "guitar", "shoes", "socks", "sandals", "beer", "horse", "camera", "lamp" , "banana", "stuff", "toy", "dog", "cat", "bird", "dinosaur", "HDMI-cable"]
versions = ["", "L", "XL", "XXL", "M", "S", "XS", "XXS", "premium", "nice-edition", "cool", "Model S", "Model T", "All inclusive", "genius edition", "paper", "gold", "copper", "silver", "platin", "10 year anniversary edition"]

categories = ["kitchen", "toys", "bath", "work", "living room", "transportation", "outdoor", "food", "accessoires"]

firstNames= ["Alex", "Alexander", "Aron", "Aaron", "Anne", "Anke", "Beate", "Benjamin", "Björn", "Bryan", "Beatrix", "Bianka", "Bob", "Cirilla", "Dan", "Daniel", "Dorina", "David", "Daisy", "Etna", "Florian", "Gerd", "Hugo", "Ina", "Julia", "Klaus", "Armin", "Chantalle"]
surNames=["Müller", "Rehn", "Schmidt", "Miller", "Witt", "Mann", "Kästner", "Direck", "Hummer", "Himmel", "Neusorge", "Altfriede", "Flint", "Boll", "Scholl", "Kante", "Roth", "Hermann", "Britt", "Narb", "Luchs", "Bruder", "Grimm", "Schelm", "Neumann"]

towns = ["Rostock", "Anklam", "Lübeck", "Berlin", "Hamburg", "Bremen", "Jena", "Angermünde", "Bochum", "Dortmund", "Kiel", "Stuttgard", "München", "Schwerin", "Potsdam", "Karlsruhe"]
streets = ["Hauptstraße", "Badstraße", "Turmstraße", "Goethestraße", "Opernplatz", "Seestraße", "Leipnizplatz", "Schlossallee", "Albert-Einstein-Straße", "Neuer Markt", "Kaminstraße", "Limonadenstraße"]

username_firsts=["crazy","lazy","cool","x", "funny", "friendly", "dark", "jk", "the" + ""]
username_lasts=["Horse", "Steve", "Noob", "Terminator", "Fly", "Chameleon", "Ralph", "CarFreak", "Student", "Dude", "Barbie", "Goomba", "Hat", "Lover", "Hater", "Troll", "Master", "Druid", "Lama", "Dog"]
ratingTexts = ["sehr gut", "furchtbar" ,"könnte besser sein" , "ok" , "Lieferung dauerte viel zu Lange", "einfach perfekt"]

people=[]
accounts=[]
for i in range(100):
    people.append(person(firstNames[random.randrange(0,len(firstNames))], surNames[random.randrange(0,len(surNames))], towns[random.randrange(0,len(towns))], streets[random.randrange(0,len(streets))], random.randrange(1, 74), datetime.datetime(random.randrange(1950,2000), random.randrange(1,13),random.randrange(1,29))))
    if random.random() <= 0.5:
        accounts.append(account(username_firsts[random.randrange(0,len(username_firsts))] + username_lasts[random.randrange(0,len(username_lasts))] + str(random.randrange(1, 100)), "password", people[i].firstname, people[i].surname))

productss=[]
for i in range(100):
    price = random.random()*100
    productss.append(product(products[random.randrange(0,len(products))], versions[random.randrange(0,len(versions))], float('%.2f'%price), categories[random.randrange(0,len(categories))]))

orders = []
for i in range(random.randrange(80, 160)):
    orders.append(random.sample(productss,random.randrange(4,10)))


mydb = mysql.connector.connect(
  host="localhost",
  user="python",
  password="1234",
  database="main_database"
)

mycursor = mydb.cursor()




#database = get_database("MainDatabase")

mycursor.execute("CREATE Table Articles(productID int PRIMARY KEY, name varchar(255), version varchar(255), price FLOAT, category varchar(255))")
mycursor.execute("CREATE Table Customers(customerID int PRIMARY KEY, firstname varchar(255), surname varchar(255), town varchar(255), street varchar(255), number INT, date_of_birth DATE)")

mydb.commit()

sql = "INSERT INTO Customers (customerID, firstname, surname, town, street, number, date_of_birth) VALUES (%s, %s, %s, %s, %s, %s, %s)"

i=0
for person in people:
#    customer = {"customer_id": i, "firstname": person.firstname, "surname": person.surname, "town": person.town, "street": person.street, "number": person.number, "date of birth": person.dob}
#    database["customers"].insert_one(customer)
#    print(person.firstname + " aus " + person.town)
#    print(person.dob)
    val = (i, person.firstname, person.surname, person.town, person.street, person.number, person.dob)
    mycursor.execute(sql, val)
    i+=1



sql = "INSERT INTO Articles (productID, name, version, price, category) VALUES (%s, %s, %s, %s, %s)"
i=0
for product in productss:
    #item = {"product_id": i, "name": product.name, "version": product.version, "price": product.price, "category": product.category}
    #database["articles"].insert_one(item)
    #print(item)
    val = (i, product.name, product.version, product.price, product.category)
    mycursor.execute(sql, val)
    i+=1

mydb.commit()


database = get_MongoDatabase("OnlineStore").database

i=0
for account in accounts:
    userAccount={"username": account.username, "password": account.password, "firstname": account.firstname, "surname": account.surname}
    database["accounts"].insert_one(userAccount)
    print(userAccount)
    i+=1

for i in range(50):
    username = accounts[random.randrange(0,len(accounts))].username
    rating = random.randrange(1, 6)
    product = productss[random.randrange(0,len(productss))].name + " " + productss[random.randrange(0,len(productss))].version
    if random.random() <= 0.5:
        text = ratingTexts[random.randrange(0,len(ratingTexts))]
    else:
        text=""
    print(username + ": " + str(rating) + " for " + product)
    if text != "":
        rating = {"rating_id": i, "username": username, "product": product, "rating": rating, "text": text}
        database["ratings"].insert_one(rating)
        print(text)
    else:
        rating = {"rating_id": i, "username": username, "product": product, "rating": rating}
        database["ratings"].insert_one(rating)

neo4jdatabase = getNeo4JDatabase("bolt://localhost:7687", "python", "12345")

for account in accounts:
    userAccount={"username": account.username, "firstname": account.firstname, "surname": account.surname}
    neo4jdatabase.session().write_transaction(add_Node, "Account", userAccount)

for product in productss:
    item = {"name":product.name, "version": product.version}
    neo4jdatabase.session().write_transaction(add_Node, "Item", item)

i = 0
for order in orders:
    orderdict = {"id":i}
    i+=1
    neo4jdatabase.session().write_transaction(add_Node, "Order", orderdict)
    for product in order:
        item = {"name":product.name, "version": product.version}
        neo4jdatabase.session().write_transaction(add_relation, "enthält", orderdict, item, "right")

    account = random.choice(accounts)
    userAccount={"username": account.username, "firstname": account.firstname, "surname": account.surname}
    neo4jdatabase.session().write_transaction(add_relation, "kaufte", userAccount, orderdict, "right")


closeNeo4JDatabase(neo4jdatabase)

