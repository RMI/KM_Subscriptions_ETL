import mysql.connector
from mysql.connector import errorcode
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import sqlalchemy
import os
from dotenv import load_dotenv
import re
import numpy as np
from datetime import date
from sqlalchemy import text
from func_tagging import func_tagging, get_tags


###### PURPOSE ############
###### Used to re-assign tags after implementing character limit on description ##########
####### Can be used to implement a recurring tag re-evaluation on the entire database ####

#Load  credentials
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
    result = conn.execute(text("select id, title, source, description from portal_live where tag_score IS NOT NULL and pubDate > '2024-01-01'"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()


# name = 'Data/backups/database_backup'+ str(date.today()) + '.csv'
# df1.to_csv(name)
df = df1

print(len(df))

# Get tag ref



tag_ref = get_tags()

print(tag_ref)
print(tag_ref.head())
#df_testing = df1[0:1000]


#df = df_testing
# Manaul backup import
#df = pd.read_excel('Data/backups/database_backup_05092023.xlsx')


# df = df[['id', 'title', 'file_title','pubDate', 'url', 'creators', 'description', 'source', 'pubName', 
#         'doi', 'journalID', 'requested', 'request_date', 'flagship', 'url_full_txt', 'date_added', 'date_updated']]

###################

with database_connection.connect() as conn:
    result = conn.execute(text("select tag_cat, tag, phrase from ref_content_tags where newsroom = 'Yes'"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

tag_ref = df1

# tag_ref = pd.read_excel('tags.xlsx', index_col = 'ID', dtype = str)
tag_ref['phrase'] = tag_ref.phrase.str.lower()

print(tag_ref.head())

# news = df
# news['description'].fillna(news['title'], inplace=True)

# # Generate average description length by source
# desc_len = news[['description', 'source']]
# desc_len['char_len'] = desc_len['description'].str.len()
# len_avg = desc_len.groupby(['source']).mean(numeric_only = True)
# # Average description length across sources
# char_limit = len_avg['char_len'].mean().round()
# char_limit = int(char_limit)
# # create a description field for tag matching in lower case and capped at average description length
# news['desc_match'] = news['description'].str[:char_limit].str.lower()

# # Define string matching function
# def get_matching_values(row, keywords):
#     matching_values = {keywords_to_tag[keyword] for keyword in keywords if row.lower().find(keyword) != -1}
#     return ','.join(matching_values) if matching_values else ''

# subset news df for testing to 2000 rows
# Create output df

news_output = pd.DataFrame()

# batch news into groups of 1000 rows
length = len(df)
batch = 500
batches = length // batch
if length % batch != 0:
    batches += 1

# Loop through batches
for i in range(batches):
    start = i * batch
    end = (i + 1) * batch
    news_sub = df[start:end]

    news = func_tagging(news_sub)

    # # Loop through descriptions, stringing together all matching tags
    # for i in tag_ref['tag_cat']:
    #     keywords_tag = tag_ref[tag_ref['tag_cat'] == i]
    #     keywords_tag = keywords_tag[['tag', 'phrase']]
    #     keywords_tag.rename(columns={"phrase":"keyword"}, inplace=True)
    #     keywords_to_tag = keywords_tag.set_index('keyword')['tag'].to_dict()
    #     news_sub[i] = news_sub['desc_match'].apply(get_matching_values, keywords= keywords_to_tag.keys())

    # append to output   
    news_output = pd.concat([news_output, news], axis=0, ignore_index=True)
    print(len(news_output))


news.rename(columns={'Adaptation':'adaptation','Behavior':'behavior', 'Emissions':'emissions', 'Environment':'environment', 
            'Finance':'finance','Geography':'geography','Industry':'industry', 'Intervention':'intervention', 'Policy':'policy', 
            'Sector':'sector', 'Technology':'technology','Theory of Change':'theory','Climate Summits/Conferences':'climate_events', 
                         'Organizational Components':'org_comp','Current Issues':'current_issues', 'tag':'tag_concat', 'value':'tag_score'}, inplace=True)


news = news[['id', 'title', 'description', 'source',
         'adaptation','behavior', 'emissions', 'environment','finance','geography','industry', 'intervention', 'policy', 'sector', 
         'technology', 'theory','climate_events','org_comp', 'current_issues', 'tag_concat', 'tag_score']]


# filter to only those with tags
news = news[news['tag_concat'] != '']


# Delete all records from database
# with database_connection.connect() as conn:
#     result_del = conn.execute(text("delete from portal_live"))


df = news


# change tag_score to integer
df['tag_score'] = df['tag_score'].astype(int)


# batch df into groups of 1000 rows
#length = len(df)

batch = 500
batches = length // batch
if length % batch != 0:
    batches += 1

# Loop through batches
for i in range(batches):
    start = i * batch
    end = (i + 1) * batch
    news_sub = df[start:end]

    # Write the dataframe to the database
with database_connection.connect() as conn:
    for index, row in df.iterrows():
        conn.execute(text("update portal_live set current_issues = :current_issues, tag_concat = :tag_concat, tag_score = :tag_score where id = :id"), 
                    {'current_issues': row['current_issues'], 'tag_concat' : row['tag_concat'], 'id' : row['id'], 'tag_score' : row['tag_score']})
      #  conn.commit()

conn.close()
database_connection.dispose()

# df.to_sql(con=database_connection, name='portal_live', if_exists='append', index=False)

# Close connections

