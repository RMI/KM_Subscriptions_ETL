import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np
from datetime import date

load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')


database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = 'rmi-temp-mysql.mysql.database.azure.com'
database_name     = 'rmi_km_news'
backup_db_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


with backup_db_connection.connect() as conn:
    result = conn.execute("select * from portal_live")
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()


name = 'Data/backups/database_recovery'+ str(date.today()) + '.xlsx'
df1.to_excel(name)
df = df1


# Delete all records from database
with backup_db_connection.connect() as conn:
    conn.execute("delete from portal_live where date_added < '2023-08-09 00:00:00'")


with backup_db_connection.connect() as conn:
    conn.rollback_prepared()

df1.to_sql(con=backup_db_connection, name='portal_live', if_exists='append', index=False)



#############
########## CONNECT TO PRODUCTION DBASE AND IMPORT RECOVERY DATA 

database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = 'rmi-prod-mysql.mysql.database.azure.com'
database_name     = 'rmi_km_news'
db_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

with db_connection.connect() as conn:
    result = conn.execute("select * from portal_live where date_added < '2023-05-09'")
    prod_old = pd.DataFrame(result.fetchall())
    prod_old.columns = result.keys()


existing_id = prod_old[['id']]

recovery_id = df1[['id']]

for i in existing_id:
    if i in recovery_id:
        existing_id['test'] = True

    else:
        existing_id['test'] = False

print(existing_id['test'].value_counts)

fa = existing_id[existing_id['test'] == False]

print(len(fa))

# Delete rows before 5/9 from prod and load recovery data
with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live where date_added < '2023-05-09'")
    re = result.fetchall()
    print(re)
# 25801 old rows

with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live")
    re = result.fetchall()
    print(re)
# 27388 total rows

# should have 1587 after delete
# then have 27405 after adding recovery data back

with db_connection.connect() as conn:
    conn.execute("delete from portal_live where date_added < '2023-05-09'")

with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live where date_added < '2023-05-09'")
    re = result.fetchall()
    print(re)

with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live")
    re = result.fetchall()
    print(re)

df1.to_sql(con=db_connection, name='portal_live', if_exists='append', index=False)

with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live where date_added < '2023-05-09'")
    re = result.fetchall()
    print(re)

with db_connection.connect() as conn:
    result = conn.execute("select count(*) from portal_live")
    re = result.fetchall()
    print(re)


conn.close()
