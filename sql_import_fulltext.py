import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np
from func_tagging import func_tagging
###### PURPOSE ############
###### Pull full-text additions from SharePoint Excel file, assign tags, and import to database
##### Description must be complete in the spreadsheet for the row to be tagged and imported. Null descriptions are excluded

#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP')


df = pd.read_excel("C:/Users/ghoffman/OneDrive - RMI/Knowledge Resources/Manual Resource Submissions.xlsx")

df = df[['id', 'title', 'pubDate', 'description', 'source','url', 'url_full_txt', 'request_email']]

df = df[df['description'].notna()]

#########################################################
################# Data Tagging ##########################
#########################################################

news = func_tagging(df)


news.rename(columns={'Adaptation':'adaptation','Behavior':'behavior', 'Emissions':'emissions', 'Environment':'environment', 
            'Finance':'finance','Geography':'geography','Industry':'industry', 'Intervention':'intervention', 'Policy':'policy', 
            'Sector':'sector', 'Technology':'technology','Theory of Change':'theory', 'Climate Summits/Conferences':'climate_events', 
                         'Organizational Components':'org_comp','tag':'tag_concat', 'value':'tag_score'}, inplace=True)

news = news[[ 'title', 'pubDate', 'url', 'url_full_txt', 'description', 'source','adaptation','behavior', 'emissions', 
             'environment','finance','geography','industry', 'intervention', 'policy', 'sector', 'technology', 'theory',
             'climate_events','org_comp','tag_concat', 'tag_score', 'request_email']]


########### Title Format #############

# remove null titles
news = news[news['title'].notnull()]

# Replace special characters that can create problems when adding full text to SharePoint
news['file_title'] = news['title'].str.replace('[\/<>*"?|]', "", regex=True)
news['file_title'] = news['file_title'].str.replace('[:]', "-", regex=True)
news['file_title'] = news['file_title'].str[:150].str.strip()

# Add URL for full text request
news['url_request'] = 'https://apps.powerapps.com/play/e/default-8ed8a585-d8e6-4b00-b9cc-d370783559f6/a/1e7fab62-974b-4a8c-8d40-b002ba18a5a9?art=' + news['file_title']

df = news

df.to_excel('Data/backups/full_text_submission_update.xlsx')

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

# Remove all matching titles from import dataframe
df_import = df
df_import.set_index('title')
df_import = df_import.drop(df_import[df_import.title.isin(final_result)].index.tolist())
df_import.reset_index()

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

print('Full Text Import Complete: ' + str(len(df_import)) + ' articles imported')