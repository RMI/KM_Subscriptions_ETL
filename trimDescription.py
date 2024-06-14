# Description: This script trims the description of newsroom content to remove the title from the description.
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from sqlalchemy import text
import urllib.parse

load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_ip = os.getenv('DBASE_IP_DEV')

database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                            format(database_username, database_password, 
                                                    database_ip, database_name))

# Get existing wide tag content
with database_connection.connect() as conn:
    result = conn.execute(text("select id, title, description from portal_live where tag_concat IS NOT NULL and date_added > '2024-04-01'"))
    df = pd.DataFrame(result.fetchall())

    df.columns = result.keys()

# Function to clean description
def clean_description(row):
    title = row['title']
    description = row['description']
    
    if description.startswith(title):
        # Check if there is more text beyond the title
        remaining_text = description[len(title):].strip()
        if remaining_text:
            description = remaining_text
    
    return description

# Apply the function to the DataFrame
df['description'] = df.apply(clean_description, axis=1)

# trim whitespace and "." for beginning of description

df['description'] = df['description'].str.lstrip('.')
df['description'] = df['description'].str.lstrip()

print(df['description'].head())
# print length where description is null, should be 0
print(len(df[df['description'].isnull()]))

print(len(df))


with database_connection.connect() as conn:
    for index, row in df.iterrows():
        conn.execute(text("update portal_live set description = :description where id = :id"), 
                     {'description' : row['description'], 'id' : row['id']})
        conn.commit()




with database_connection.connect() as conn:
    result = conn.execute(text("select id, file_title from portal_live where tag_concat IS NOT NULL and date_added > '2024-05-01'"))
    df = pd.DataFrame(result.fetchall())

    df.columns = result.keys()

url = 'https://apps.powerapps.com/play/e/default-8ed8a585-d8e6-4b00-b9cc-d370783559f6/a/1e7fab62-974b-4a8c-8d40-b002ba18a5a9?art='

# encode file_title
df['safe_string'] = df['file_title'].apply(urllib.parse.quote_plus)


# concat url to file_title and encode
df['url_request'] = url + df['safe_string']

print(df['url_request'][1])


with database_connection.connect() as conn:
    for index, row in df.iterrows():
        conn.execute(text("update portal_live set url_request = :url_request where id = :id"), 
                     {'url_request' : row['url_request'], 'id' : row['id']})
        conn.commit()

# Close connections
database_connection.dispose()
conn.close()