
# This script imports tag profiles from Excel files in the Tag_Profiles folder into the rmi_km_news database.

import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy
from pathlib import Path
from sqlalchemy import text
from datetime import date

#Load database credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP')

rmi_ip_dev = os.getenv('DBASE_IP_DEV')

# Condition to add data to dev or production database
# prompt in terminal to select database and wait for user input
database = input('Enter database (dev or prod): ')

# don't continue until user enters 'dev' or 'prod'
while database != 'dev' and database != 'prod':
    database = input('Enter database (dev or prod): ')

if database == 'dev':
    rmi_ip = rmi_ip_dev

# Database connection

database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


# Point to data folder
mydir = Path("Tag_Profiles/")

# Create blank dataFrame to load new data
df = pd.DataFrame(columns=['tag_cat', 'tag', 'program'])

# Loop through data folder, appending any Excel files to df
for file in mydir.glob('*.xlsx'):
    df_data = pd.read_excel(file)
    df = pd.concat([df, df_data])

# write out df, 
df.to_excel('profiles.xlsx')

# Pull all tags from database
with database_connection.connect() as conn:
    result = conn.execute(text("select guid, tag from ref_content_tags"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()


# Merge df with df1 to get tag GUIDs. These are unique to each tag and will be used to join to the content_tags table
df2 = pd.merge(df, df1, how='left', left_on='tag', right_on='tag')

print(df2.head())

# rename columns
df2.rename(columns={'guid': 'tag_guid', 'program': 'cost_center'}, inplace=True)

# drop tag column
df2.drop(columns=['tag', 'tag_cat'], inplace=True)

# drop duplicate tag_guids
df2.drop_duplicates(subset=['tag_guid'], inplace=True)

# write to database
df2.to_sql('tag_profiles', con=database_connection, if_exists='append', index=False)

# move excel files to archive folder
for file in mydir.glob('*.xlsx'):
    file.rename('Tag_Profiles/archive/' + file.name + 'imported' + date.today().strftime('%Y%m%d') + '.xlsx')