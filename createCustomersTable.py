import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433", 
  database="mysqlDB"          # connect to  "mysqlDB" automatically
)

mycursor = mydb.cursor()


# create customers table 
mycursor.execute("CREATE TABLE customers (Customer_ID INT AUTO_INCREMENT PRIMARY KEY, firstName VARCHAR(255), lastName VARCHAR(255))")