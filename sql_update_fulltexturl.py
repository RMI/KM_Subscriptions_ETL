import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np

###### PURPOSE ############
###### Used to update full text urls in database at the end of each day and prior to the daily resource update
###### URLs are added to spreadsheet automatically when librarian uploads a new file to Full Text SP folder

#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP')

# Get new urls from SharePoint

url_update = pd.read_excel('C:/Users/ghoffman/OneDrive - RMI/Knowledge Resources/Available Resources.xlsx')

url_update = url_update[['id', 'url_full_txt', 'request_date', 'requested']]
url_update = url_update[url_update['url_full_txt'].notna()]
url_update['request_date'] = pd.to_datetime(url_update['request_date'] , format="%Y-%m-%d %H:%M:%S")
url_update['request_date'] = url_update['request_date'].dt.tz_localize(None)


database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


with database_connection.connect() as conn:
   conn.execute("delete from url_update")

url_update.to_sql(con=database_connection, name='url_update', if_exists='append', index=False)

# Close connections
database_connection.dispose()

# connect to database
config = {
  'host': rmi_ip,
  'user':'rmiadmin',
  'password': rmi_db,
  'database':'rmi_km_news',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': 'C:/Users/ghoffman/OneDrive - RMI/01. Projects/DigiCertGlobalRootCA.crt.pem'
}
# Construct connection string
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

query_update = """UPDATE portal_live, url_update SET portal_live.url_full_txt = url_update.url_full_txt, portal_live.request_date = url_update.request_date, portal_live.requested = url_update.requested WHERE portal_live.id = url_update.id;"""

try:
   # Execute the SQL command
   cursor.execute(query_update)   
   # Commit your changes in the database
   conn.commit()
except:
   # Rollback in case there is any error
   conn.rollback()
   
#Closing the connection
conn.close()