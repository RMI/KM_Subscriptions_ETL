import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv

#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP')

df = pd.read_excel('news_data.xlsx')

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

# get all titles from database
cursor.execute("SELECT title from portal_live;")
final_result = [i[0] for i in cursor.fetchall()]

# titles to lower for comparison to new resources
final_result = list(map(str.lower,final_result))

# Remove all matching titles from import dataframe
df_import = df
df_import.set_index('title')
df_import = df_import.drop(df_import[df_import.title.str.lower().isin(final_result)].index.tolist())
df_import.reset_index(drop=True, inplace=True)

# remoove Unamed: 0 column
df_import = df_import.drop('Unnamed: 0', axis=1)

# close connection
cursor.close()

# Import dataframe into MySQL
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


df_import.to_sql(con=database_connection, name='portal_live', if_exists='append', index=False)

# Close connections
database_connection.dispose()
conn.close()

print('Database Import Complete: ' + str(len(df_import)) + ' articles imported')
