import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433", 
  database="mysqldb"
)

mycursor = mydb.cursor()

# delete record from mySQL
sql_ = "DELETE FROM customers WHERE Customer_ID = 4"
mycursor.execute(sql_)

# commit the changes
mydb.commit()  