import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from datetime import date
from sqlalchemy import text


file = 'Data/backups/tag_import' + str(date.today()) + '.xlsx'
###### PURPOSE ############
# Transform wide, comma separated tag data into long format and import to MySQL.

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

# Get existing wide tag content
with database_connection.connect() as conn:
    result = conn.execute(text("select id, adaptation,behavior, emissions, environment,finance,geography,industry, intervention, policy, sector, technology, theory, climate_events, org_comp from portal_live where tag_concat IS NOT NULL"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

# Get existing long format ids
with database_connection.connect() as conn:
    result = conn.execute(text("select content_id from portal_content_tags"))
    df_check = pd.DataFrame(result.fetchall())
    df_check.columns = result.keys()

# cross check to filter out wide format that have already been converted
df = df1[~df1.id.isin(df_check['content_id'])]

# Transform wide tag categories to long format and remove Null
df_import = pd.melt(df, id_vars= 'id', value_vars= df[1:len(df.columns)], value_name='tag', var_name='tag_cat')
df_import = df_import[df_import['tag'].notnull()]
df_import = df_import[df_import['tag'] != '']

# Split comma separated tags and join back to id and category
df_tags = df_import['tag'].str.split(",", expand=True)
df_import = df_import.drop('tag', axis=1)
df_import_f = pd.concat([df_import, df_tags], axis=1)

# Transform wide tags to long format, preserving id and tag category
df_import_f2 = pd.melt(df_import_f, id_vars= {'id', 'tag_cat'}, value_vars= df_import_f[2:len(df_import_f.columns)], value_name='tag', var_name='tag_count')
df_import_f2 = df_import_f2[df_import_f2['tag'].notnull()]
df_import_f2 = df_import_f2[df_import_f2['tag'] != '']
df_import_f2.rename(columns={'id':'content_id'}, inplace=True)
df_import_f2 = df_import_f2.drop('tag_count', axis=1)

# Write out backup and import to MySQL
df_import_f2.to_excel(file)

df_import_f2.to_sql(con=database_connection, name='portal_content_tags', if_exists='append', index=False)

# Close connections
database_connection.dispose()
conn.close()
