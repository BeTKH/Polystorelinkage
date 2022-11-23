import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="test1234", 
  database="mysqldbs"          # connect to  "mysqldbs" automatically
)

mycursor = mydb.cursor()


# create customers table 
mycursor.execute("CREATE TABLE customers (Customer_ID INT AUTO_INCREMENT PRIMARY KEY, firstName VARCHAR(255), lastName VARCHAR(255))")

