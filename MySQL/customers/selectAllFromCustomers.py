import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433", 
  database="mysqldb"
)

mycursor = mydb.cursor()

# select evtg from customers table
mycursor.execute("SELECT * FROM customers")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)