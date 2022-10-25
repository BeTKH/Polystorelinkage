# Get the database using the method we defined in pymongo_test_insert file 
from pymongo_test_insert import get_database
dbname = get_database()

# Create a new collection
collection_name = dbname["user_1_items"]

category_index = collection_name.create_index("category")

item_details = collection_name.find({"category" : "food"})

from pandas import DataFrame
# convert the dictionary objects to dataframe
items_df = DataFrame(item_details)

# see the magic
print(items_df)



