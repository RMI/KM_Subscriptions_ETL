# Goal of this script is to pull data from Monday.com API to get published knowledge cards
# Currently using admin token from Kevin, will need to obtain dedicated KM token in the future

import requests
import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
from time import sleep
from random import randint

load_dotenv('cred.env')
monday_token = os.getenv('MONDAY_TOKEN')
rmi_db_ip = os.getenv('DBASE_IP')
rmi_db = os.getenv('DBASE_PWD')

# database connection
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

def get_monday_call(x):
    headers = {'Authorization': monday_token}
    response = requests.post('https://api.monday.com/v2', json={'query': x}, headers=headers)
    return response.json()

# tags = ['Adaptation','Carbon','Emissions','Greenhouse Gases (GHG)','Methane','Climate Change','Rural/Farmland','Suburban',
#         'Urban','Finance','Sustainable Investment','China','Europe','Global South','India','United States','Education',
#         'Carbon Markets','Climate Policy','Inflation Reduction Act','Buildings','Energy','Food','Industries','Mobility',
#         'Waste','Carbon Capture (CCS)','Carbon Capture (Nature)','Distributed Electricity','Distributed Generation',
#         'Charging Infrastructure','Electric Vehicles','Electricity Grid','Emissions Reduction Technology','Energy Efficiency',
#         'Hydrogen','Hydropower','Micro/Mini Grid','Natural Gas','Nuclear','Oil','Solar','Supply Chain Emissions',
#         'Sustainable Transportation','Wind','Technical/Techno-Economic Modeling','Fleet Electrification']

# # sort tags alphabetically
# tags.sort()

# print(tags)

# for i in tags:
#     query = "mutation {change_simple_column_value(item_id: 6445932955, board_id:4864438534, column_id: \"dropdown__1\", value: \"" + i +"\", create_labels_if_missing: true) {id}}"
#     res = [get_monday_call(query)]

# print(res)


# Initial query to get first 25 items
query = "query {boards (ids: 4864438534){items_page (limit: 25, query_params:{rules:{column_id: \"date4\", compare_value: [\"\"], operator:is_not_empty}}) {cursor items { name column_values(ids: [\"name\",\"long_text05\", \"status\", \"link\",\"date4\",\"dropdown__1\"]){column {title} value text} }} }}"
res = [get_monday_call(query)]

# print(res['data']['boards'][0].keys())

# Get cursor for next page
cursor = res[0]['data']['boards'][0]['items_page']['cursor']
cursor_new = "\"" + cursor + "\""

# page through remaining values until cursor is None
while cursor_new is not None:
    query = "query {boards (ids: 4864438534){items_page (limit: 25, cursor: " + cursor_new + ") {cursor items { name column_values(ids: [\"name\",\"long_text05\", \"status\", \"link\",\"date4\",\"dropdown__1\"]){column {title} value text} }} }}"
    res_next = get_monday_call(query)
    res.append(res_next)
    cursor = res_next['data']['boards'][0]['items_page']['cursor']
    cursor_new = "\"" + cursor + "\""
    print(cursor_new)
    sleep(randint(1, 3))

# create dataframe with columns from monday board
columns = []
for i in range(0,5):
    column = res[0]['data']['boards'][0]['items_page']['items'][1]['column_values'][i]['column']['title']
    columns.append(column)

df = pd.DataFrame(columns= columns)
df['Name'] = ''


# for each list item in res, get the items and append to dataframe
for i in range(len(res)):
# 0 is status
# 1 is link
# 2 is date
# 3 is description
# Get column names
    items = res[i]['data']['boards'][0]['items_page']['items']
# Get values and concat to dataframe
    for t in range(len(items)):
        status = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][0]['value']
        link = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][1]['value']
        date = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][2]['value']
        description = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][3]['value']
        tags = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][4]['text']
        name = items[t]['name']
        df = df.append({'Name': name,'Status': status, 'Link to page': link, 'Date Published': date, 'Brief Card Description': description, 'Choose Tags': tags}, ignore_index=True)

#print(res[0]['data']['boards'][0]['items_page']['items'][3]['column_values'][4]['text'])

# Define a regular expression for URLs
url_pattern = r'(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
# Extract URLs from the 'Link to page' column
df['url'] = df['Link to page'].str.extract(url_pattern, expand=False)

# extract Date from Date Published
date_pattern = r'(\d{4}-\d{2}-\d{2})'
df['pubDate'] = df['Date Published'].str.extract(date_pattern, expand=False)

# Extract text from the 'Brief Card Description' column
df['description'] = df['Brief Card Description'].str.extract(r'([A-Za-z].*)', expand=False)

# Drop unneeded columns and rename
df = df.drop(columns=['Link to page', 'Date Published', 'Status', 'Brief Card Description'])
df.rename(columns={'Name': 'title', 'Choose Tags':'tags'}, inplace=True)

# drop null URL
df = df.dropna(subset=['url'])

# replace None tag with empty string
df['tags'] = df['tags'].fillna('')

# Join tags back to df
#df_import = pd.merge(df, df_tagging, how='left', left_on='title', right_on='title')

df_import = df
df_import.rename(columns={'tags':'content_tags'}, inplace=True)

# Delete existing data in the database
with database_connection.connect() as conn:
    conn.execute(text("DELETE FROM know_cards"))
    conn.commit()

# Write the dataframe to the database
df_import.to_sql('know_cards', con=database_connection, if_exists='append', index=False)

df.rename(columns={'content_tags':'tags'}, inplace=True)

# Split comma separated tags and join back to id and category
df_tags = df['tags'].str.split(",", expand=True)

df_tagging = pd.concat([df['title'], df_tags], axis=1)

# Transform wide tags to long format, preserving id and tag category
df_tagging = pd.melt(df_tagging, id_vars={'title'}, value_vars=df_tagging[1:len(df_tagging.columns)], value_name='tag', var_name='tag_count')
df_tagging = df_tagging[df_tagging['tag'].notnull()]

df_tagging = df_tagging[df_tagging['tag'] != '']
df_tagging = df_tagging.drop(columns={'tag_count'}, axis=1)

df_tagging['tag'] = df_tagging['tag'].str.strip()


# get tag guid from database
    # Add tag GUID from database
with database_connection.connect() as conn:
    result = conn.execute(text("select tag, tag_cat, guid from ref_content_tags"))
    df_tag_id = pd.DataFrame(result.fetchall())
    df_tag_id.columns = result.keys()

# Merge tag_guid to df_tagging
df_tagging = pd.merge(df_tagging, df_tag_id, how='left', left_on='tag', right_on='tag')

# Drop tag and tag_cat columns and rename guid to tag_guid
df_tagging = df_tagging.drop(columns={'tag_cat'})
df_tagging.rename(columns={'guid':'tag_guid'}, inplace=True)

# drop where title and tag_guid are duplicates
df_tagging = df_tagging.drop_duplicates(subset=['title', 'tag_guid'])


with database_connection.connect() as conn:
    conn.execute(text("DELETE FROM know_cards_tags"))
    conn.commit()

# import data to database
df_tagging.to_sql('know_cards_tags', con=database_connection, if_exists='append', index=False)

print('Knowledge Catalog Data imported to database')