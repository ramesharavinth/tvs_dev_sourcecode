from pymysql import Error
import pymysql

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='12345', db='freemanDB')

cur = conn.cursor()
cur.execute("SELECT * FROM pet")

sql = "INSERT INTO pet (name,owner,species,sex,birth,death) VALUES(%s, %s, %s, %s, %s, %s)"
val = ('Puffball1','Diane1','hamster1','f','1999-03-30','1999-03-30')

try:
   # print(query)
  cur.execute(sql, val)
except Error as e:
    print('Error:', e)
conn.commit()

# print(cur.rowcount)

# for row in cur:
#     print(row)

cur.close()
conn.close()