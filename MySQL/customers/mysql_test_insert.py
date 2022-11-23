import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="test1234"
)

print(mydb)

# creating SCHEMA named 'mysqldbs'

mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE mysqldbs")  # creates schema with Tables, Views, Stored Procedures and Functions 