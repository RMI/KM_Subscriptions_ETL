# import packages
import pandas as pd
import requests
import os
from os import makedirs
from os.path import join, exists
from dotenv import load_dotenv
import json
from datetime import date, timedelta
import glob

#Load API credentials
load_dotenv('cred.env')
API = os.getenv('GUARDIAN_API')

# Create date variable for queries
today = date.today()
past = date.today() - timedelta(days=5)

# API credentials
MY_API_KEY = API
API_ENDPOINT = 'http://content.guardianapis.com/search'
# Section list. Can add to this list to pull more sections
section = ['environment', 'science', 'technology']

# Create dictionary to store articles
ARTICLES_DIR = join('tempdata', 'articles')
makedirs(ARTICLES_DIR, exist_ok=True)

# Sample URL
# http://content.guardianapis.com/search?from-date=2016-01-02&
# to-date=2016-01-02&section=environment&order-by=newest&show-fields=all&page-size=200&api-key=your-api-key-goes-here

# Loop through sections, dates, and pages to pull articles for the past 5 days
for i in section:
    my_params = {
        'section': i,
        'from-date': "",
        'to-date': "",
        'order-by': "newest",
        'show-fields': 'all',
        'page-size': 200,
        'api-key': MY_API_KEY
    }

    # day iteration from here:
    # http://stackoverflow.com/questions/7274267/print-all-day-dates-between-two-dates
    start_date = past
    end_date = today
    dayrange = range((end_date - start_date).days + 1)
    for daycount in dayrange:
        dt = start_date + timedelta(days=daycount)
        datestr = dt.strftime('%Y-%m-%d')
        fname = join(ARTICLES_DIR, datestr + i + '.json')
        if not exists(fname):
            # then let's download it
            print("Downloading", datestr)
            all_results = []
            my_params['from-date'] = datestr
            my_params['to-date'] = datestr
            current_page = 1
            total_pages = 1
            while current_page <= total_pages:
                print("...page", current_page)
                my_params['page'] = current_page
                resp = requests.get(API_ENDPOINT, my_params)
                data = resp.json()
                all_results.extend(data['response']['results'])
                # if there is more than one page
                current_page += 1
                total_pages = data['response']['pages']

            with open(fname, 'w') as f:
                print("Writing to", fname)

                # re-serialize it for pretty indentation
                f.write(json.dumps(all_results, indent=2))

# Get a list of all JSON files in the directory
json_files = glob.glob(join(ARTICLES_DIR, '*.json'))

# Create an empty list to store the dataframes
dfs = []

# Iterate over each JSON file and read it into a dataframe
for file in json_files:
    with open(file, 'r') as f:
        data = json.load(f)
        df = pd.json_normalize(data)
        dfs.append(df)

# Concatenate all dataframes into one
combined_df = pd.concat(dfs, ignore_index=True)


df_export = combined_df[['webTitle', 'webPublicationDate', 'webUrl', 'fields.bodyText']]

df_export['source'] = 'The Guardian'
df_export['description'] = df_export['fields.bodyText'].str[:500]
df_export['pubDate'] = pd.to_datetime(df_export['webPublicationDate'])
df_export['pubDate'] = df_export['pubDate'].dt.date
df_export['url_full_txt'] = df_export['webUrl']
df_export_trim = df_export[['webTitle', 'url_full_txt', 'pubDate', 'description', 'source']]
df_export_trim.rename(columns={"webTitle": "title"}, inplace=True)

# Write the combined dataframe to an excel file
df_export_trim.to_excel('Data/guardian_data.xlsx')

print('Guardian API Data Extraction Complete: ' + str(len(df_export_trim)) + ' articles extracted')
