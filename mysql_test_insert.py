import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433"
)

print(mydb)

# creating a DB named 'mysql'

mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE mysqlDB")