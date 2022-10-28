from neo4j import GraphDatabase

def getNeo4jDatabase():

    # mongodb+srv://<username>:<password>@mongodbunireg-dfg.hevckkf.mongodb.net/test  username: beck_tkh password: 1433

    CONNECTION_STRING = "mongodb+srv://beck_tkh:1433@cluster0.xjbdvhx.mongodb.net/test"

    client = MongoClient(CONNECTION_STRING)

    return client['user_shopping_list']

    if __name__ == "__main__":

        dbname = get_database()