import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433", 
  database="mysqlDB"
)

mycursor = mydb.cursor()

# insert single row into customers table 

#sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
#val = ("John", "Highway 21")
#mycursor.execute(sql, val)
#mydb.commit()
#print(mycursor.rowcount, "record inserted.")


# insert multiple rows into orders table 
sql = "INSERT INTO customers (orderdate, Quantity) VALUES (%s, %d)"
val = [
  ('24-06-2022', 2),
  ('29-04-2022', 1)
]

mycursor.executemany(sql, val)
mydb.commit()

print(mycursor.rowcount, "was inserted.")