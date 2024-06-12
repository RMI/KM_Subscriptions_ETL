#### Springer Journal API
# Purpose: Extract journal information from Springer for News Aggregator
# Notes: 
    # May need to expand criteria, currently runs articles published today with keywords "Climate", "Energy", and "Renewable"
    # Could try "Environment", but that is likely to get pretty broad

# Load packages
import pandas as pd
import requests
from datetime import date, timedelta
from time import strftime
import os
from dotenv import load_dotenv

#Load API credentials
load_dotenv('cred.env')
API = os.getenv('SPRINGER_API')

# Create date variable for queries
today = date.today()
past = date.today() - timedelta(days=5)

# Identify query url strings to send against the API
url = ['http://api.springernature.com/metadata/json?q=type:Journal+keyword:climate+onlinedatefrom:' + past.strftime(format = '%Y-%m-%d') + ' onlinedateto:' + today.strftime(format = '%Y-%m-%d') + '&s=1&p=99&api_key='+ API
,'http://api.springernature.com/metadata/json?q=type:Journal+keyword:renewable+onlinedatefrom:' + past.strftime(format = '%Y-%m-%d') + ' onlinedateto:' + today.strftime(format = '%Y-%m-%d') + '&s=1&p=99&api_key='+ API
,'http://api.springernature.com/metadata/json?q=type:Journal+keyword:energy+onlinedatefrom:' + past.strftime(format = '%Y-%m-%d') + ' onlinedateto:' + today.strftime(format = '%Y-%m-%d') + '&s=1&p=99&api_key='+ API]


# Create empty list to store result from each loop iteration
result_list = []
# Loop through each query url and extract the fields contained in the dataFrame above
# If run time gets too long, you could just extract all variables and the subset once after the loop, but the dataFame above will
#    need to be adjusted
for i in url:
    response = requests.get(i)
    a = response.json()
    df = pd.json_normalize(a, 'records')
    df = df[['language','title','creators', 'publicationName',  'doi',
              'publisher', 'publicationDate', 'abstract']]
    result_list.append(df)

# Drop duplicate articles
df_entries = pd.concat(result_list)
df_entries.drop_duplicates("doi", inplace=True)

# Add source field, create useful url field, and rename variables to match master data fields
df_entries['source'] = df_entries['publisher']
df_entries['url'] = 'http://dx.doi.org/' + df_entries['doi']
df_full1 = df_entries[['source', 'url', 'title', 'creators', 'doi', 'publicationName', 'publicationDate', 'abstract']]
df_full1.rename(columns={'abstract':'description', 'publicationDate':'pubDate', 'publicationName': 'pubName'}, inplace=True)

# Remove any non-English characters from description
df_full1['description'] = df_full1['description'].str.encode('ascii', 'ignore').str.decode('ascii')

# Write out data
df_full1.to_excel('Data/springer_data.xlsx')

print('Springer Journal API Data Extraction Complete: ' + str(len(df_full1)) + ' articles extracted')