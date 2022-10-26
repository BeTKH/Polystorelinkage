from connection import get_database
from dateutil import parser

dbname = get_database()

collection_name = dbname["user_1_items"]


item_1 = {
  "_id" : "U1IT00001",
  "item_name" : "Blender",
  "max_discount" : "10%",
  "batch_number" : "RR450020FRG",
  "price" : 340,
  "category" : "kitchen appliance"
}

item_2 = {
  "_id" : "U1IT00002",
  "item_name" : "Egg",
  "category" : "food",
  "quantity" : 12,
  "price" : 36,
  "item_description" : "brown country eggs"
}

# inserting many documents at once, use the pymongo insert_many() method
collection_name.insert_many([item_1,item_2])



# insert date with dateutil 
expiry_date = '2021-07-13T00:00:00.000Z'
expiry = parser.parse(expiry_date)
item_3 = {
  "_id":"whateveritis",
  "item_name" : "Bread",
  "quantity" : 2,
  "ingredients" : "all-purpose flour",
  "expiry_date" : expiry
}

#use the insert_one() method to insert a single document.
collection_name.insert_one(item_3)
