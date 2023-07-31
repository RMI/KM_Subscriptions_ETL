import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np

###### PURPOSE ############
###### Pull full-text additions from SharePoint Excel file, assign tags, and import to database
##### Description must be complete in the spreadsheet for the row to be tagged and imported. Null descriptions are excluded

#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')

df = pd.read_excel("C:/Users/ghoffman/OneDrive - RMI/Knowledge Resources/Manual Resource Submissions.xlsx")

df = df[['id', 'title', 'pubDate', 'description', 'source','url', 'url_full_txt']]

df = df[df['description'].notna()]

#########################################################
################# Data Tagging ##########################
#########################################################

tag_ref = pd.read_excel('tags.xlsx', index_col = 'ID', dtype = str)
tag_ref['phrase'] = tag_ref.phrase.str.lower()

news = df
news['description'].fillna(news['title'], inplace=True)

# Generate average description length by source
desc_len = news[['description', 'source']]
desc_len['char_len'] = desc_len['description'].str.len()
len_avg = desc_len.groupby(['source']).mean(numeric_only = True)
# Average description length across sources
char_limit = len_avg['char_len'].mean().round()
char_limit = int(char_limit)
# Institute 250 character minimum limit to make sure descriptions aren't limited to less than 250
if char_limit < 250:
  char_limit = 250 

# create a description field for tag matching in lower case and capped at average description length
news['desc_match'] = news['description'].str[:char_limit].str.lower()

# loop through and assign tags
for i in tag_ref['tag_cat']:
    values = tag_ref[tag_ref['tag_cat'] == i]
    v = list(values['phrase'])
    # conditions = list(map(news['description'].str.contains, v)) # Condition to say where the news description includes any tag
    conditions = list(map(news['desc_match'].str.contains, values['phrase'])) # Condition to say where the news description includes any tag
    # news[i] = np.select(conditions, v, '') # Apply tags, "" if no tags exist
    news[i] = np.select(conditions, values['tag'], '') # Apply tags, "" if no tags exist

# Create concatenated tag variable
news['tag'] = news[['Behavior', 'Emissions', 'Environment', 'Industry' ,'Intervention',
                         'Policy', 'Region', 'Status', 'Sector', 'Technology']].fillna('').agg(','.join, axis=1)

### Create match score variable
# Create id for unique article
news['uid'] = np.arange(0,len(news),1)

# Transform tags to long format 
score_sub = news[['uid','Behavior', 'Emissions', 'Environment', 'Industry' ,'Intervention',
                         'Policy', 'Region', 'Status', 'Sector', 'Technology']]
score = score_sub.melt(id_vars = ['uid'], ignore_index=False).reset_index()
score['value'].replace('', np.nan, inplace=True)
score = score.dropna()
# Create count of tag categories
tag_score = score.groupby('uid')['value'].count()
# Join score back to news df
news = news.join(tag_score, on='uid')

# Remove duplicate and trailing commas from null tags
pattern = re.compile(r',{2,}')
news['tag'].replace(pattern, ',', regex = True, inplace = True)

pattern = re.compile(r'(^[,\s]+)|([,\s]+$)')
news['tag'].replace(pattern, '', regex = True, inplace = True)

news.rename(columns={'Behavior':'behavior', 'Emissions':'emissions', 'Environment':'environment', 
            'Industry':'industry', 'Intervention':'intervention', 'Policy':'policy', 'Region':'region', 
            'Sector':'sector', 'Status':'status', 'Technology':'technology', 'tag':'tag_concat', 'value':'tag_score'}, inplace=True)

news = news[['title', 'pubDate', 'description', 'url', 'source', 'url_full_txt', 'behavior', 'emissions', 'environment', 
            'industry', 'intervention', 'policy', 'region', 'sector', 'status', 'technology', 'tag_concat', 'tag_score']]


########### Title Format #############

# remove null titles
news = news[news['title'].notnull()]

# Replace special characters that can create problems when adding full text to SharePoint
news['file_title'] = news['title'].str.replace('[\/<>*"?|]', "", regex=True)
news['file_title'] = news['file_title'].str.replace('[:]', "-", regex=True)
news['file_title'] = news['file_title'].str[:150]

df = news

df.to_excel('Data/backups/full_text_submission_update.xlsx')

# connect to database
config = {
  'host':'rmi-prod-mysql.mysql.database.azure.com',
 # 'host' :'rmi-prod-mysql.rmi-prod-mysql.private.mysql.database.azure.com',
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
database_ip       = 'rmi-prod-mysql.mysql.database.azure.com'
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


df_import.to_sql(con=database_connection, name='portal_live', if_exists='append', index=False)

# Close connections
database_connection.dispose()

