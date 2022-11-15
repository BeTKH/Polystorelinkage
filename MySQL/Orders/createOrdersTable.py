import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433", 
  database="mysqlDB"          # connect to  "mysqlDB" automatically
)

mycursor = mydb.cursor()


# create Orders table 
mycursor.execute("CREATE TABLE orders (Order_ID INT AUTO_INCREMENT PRIMARY KEY, orderdate DATE, Quantity INT)")