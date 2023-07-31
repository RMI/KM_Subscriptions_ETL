import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np

###### PURPOSE ############
###### Used to update file title field after deciding to limit SharePoint file names to 150 characters and removal of special characters


#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')


df = pd.read_excel('Data/database_backup_021723.xlsx')

news = df

# Replace special characters that can create problems when adding full text to SharePoint
news['file_title'] = news['title'].str.replace('[\/<>*"?|]', "", regex=True)
news['file_title'] = news['file_title'].str.replace('.$', "", regex=True)
news['file_title'] = news['file_title'].str.replace('[:]', "-", regex=True)
news['file_title'] = news['file_title'].str[:150]

df = news

df.to_excel('updated_news.xlsx')

# Import dataframe into MySQL
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = 'rmi-prod-mysql.mysql.database.azure.com'
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


df.to_sql(con=database_connection, name='portal_live', if_exists='append', index=False)

# Close connections
database_connection.dispose()
