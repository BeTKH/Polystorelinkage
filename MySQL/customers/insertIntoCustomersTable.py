import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="test1234", 
  database="mysqldbs"
)

mycursor = mydb.cursor()

# insert single row into customers table 

#sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
#val = ("John", "Highway 21")
#mycursor.execute(sql, val)
#mydb.commit()
#print(mycursor.rowcount, "record inserted.")


# insert multiple rows into customers table 
sql = "INSERT INTO customers (firstName, lastName) VALUES (%s, %s)"
val = [
  ('John', 'Allen'),
  ('Pauline', 'Koch')
]

mycursor.executemany(sql, val)
mydb.commit()

print(mycursor.rowcount, "was inserted.")
