import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np
from datetime import date

###### PURPOSE ############
###### Used to re-assign tags after implementing character limit on description ##########
####### Can be used to implement a recurring tag re-evaluation on the entire database ####

#Load API credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP')

database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


with database_connection.connect() as conn:
    result = conn.execute("select * from portal_live")
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()


name = 'Data/backups/database_backup'+ str(date.today()) + '.xlsx'
df1.to_excel(name)
df = df1

#df = df1[0:500]
# Manaul backup import
#df = pd.read_excel('Data/backups/database_backup_05092023.xlsx')


df = df[['id', 'title', 'file_title','pubDate', 'url', 'creators', 'description', 'source', 'pubName', 
        'doi', 'journalID', 'requested', 'request_date', 'flagship', 'url_full_txt', 'date_added', 'date_updated']]


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
# create a description field for tag matching in lower case and capped at average description length
news['desc_match'] = news['description'].str[:char_limit].str.lower()

# Define string matching function
def get_matching_values(row, keywords):
    matching_values = {keywords_to_tag[keyword] for keyword in keywords if row.lower().find(keyword) != -1}
    return ', '.join(matching_values) if matching_values else ''

# Loop through descriptions, stringing together all matching tags
for i in tag_ref['tag_cat']:
    keywords_tag = tag_ref[tag_ref['tag_cat'] == i]
    keywords_tag = keywords_tag[['tag', 'phrase']]
    keywords_tag.rename(columns={"phrase":"keyword"}, inplace=True)
    keywords_to_tag = keywords_tag.set_index('keyword')['tag'].to_dict()
    news[i] = news['desc_match'].apply(get_matching_values, keywords= keywords_to_tag.keys())

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

news = news[['id', 'title', 'file_title','pubDate', 'url', 'creators', 'description', 'source', 'pubName', 'doi', 'journalID',
         'behavior', 'emissions', 'environment','industry', 'intervention', 'policy', 'region', 'sector', 'status', 
         'technology', 'tag_concat', 'tag_score', 'requested', 'request_date','flagship', 'url_full_txt', 'date_added', 'date_updated',]]

df = news

df.to_excel('updated_news.xlsx')

# Delete all records from database
with database_connection.connect() as conn:
    result_del = conn.execute("delete from portal_live")




df.to_sql(con=database_connection, name='portal_live', if_exists='append', index=False)

# Close connections
conn.close()
database_connection.dispose()
