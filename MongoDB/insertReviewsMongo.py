
def get_database():
    from pymongo import MongoClient
    import pymongo

    # Provide the mongodb atlas url to connect python to mongodb using pymongo

    CONNECTION_STRING = "mongodb://localhost:27017"  # local
    #CONNECTION_STRING = "mongodb+srv://beck_tkh:1433@cluster0.mdd3cai.mongodb.net/test"  # cloud

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['PolyMongo']
    
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
    
    # Get the database
    dbname = get_database()


    collection_name = dbname["Reviews"]
    
    review_HP = {
        "productname" : "HP Monitor",
        "reviews" : [
            {"user1":"Good quality"}, 
            {"user2":[
                {"name":"Muller", 
                "comment":"fair performance for its price", 
                "rating":3.5 }
                ]}]
    }
    
    review_DELL = {
        "productname" : "Dell Desktop",
        "reviews" : [{"user1":"reasonable perfomance"}, {"user2": [{"name": "Thodros"}, {"comment":"works quite well."}]}]
    }


    collection_name.insert_many([review_HP, review_DELL])