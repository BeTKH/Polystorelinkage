import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="jsks@1433",
  database="mysqlDB"  # access "mysql" database when making the connection   --- here mysql is the name of the DB 
)



# check if DB exists

mycursor = mydb.cursor()
mycursor.execute("SHOW DATABASES")

for x in mycursor:
    print(x)
