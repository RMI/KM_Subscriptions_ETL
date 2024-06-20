### News Aggregator Data Pull

# The following script executes extractions for various news and other publication sources. Errors will be raised if an individual script fails. If this occurs, open the individual script to debug.
# If all is running well, you should only need to execute this script when updating the News Aggregator

import pandas as pd
from pathlib import Path
#import numpy as np
#import re
import win32com.client
import time
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
from func_tagging import func_tagging
from func_tag_transform import tag_transform
import urllib.parse
import subprocess
import sys

# Prompt user to select prod or dev
run_type = input('Enter "prod" or "dev" to select production or development environment: ')
run_type = run_type.lower()

#run_type = 'prod'
if run_type == 'prod':
    # Connect to VPN
    p = subprocess.Popen('powershell.exe -ExecutionPolicy RemoteSigned -file "ps_vpn_connect_rmi.ps1"', stdout=sys.stdout)
elif run_type == 'dev':
    # Connect to VPN
    p = subprocess.Popen('powershell.exe -ExecutionPolicy RemoteSigned -file "ps_vpn_connect_flex.ps1"', stdout=sys.stdout)
else:
    print('Invalid entry. Please enter "prod" or "dev"')
    exit()

# Add section to delete from portal_content_tags where content_id not in portal_live

time.sleep(5)

# Select research or newsroom only
#run_type = 'newsroom'
run_type = 'research'

#Load database credentials
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


# confirm database connection
connection = database_connection.connect()
metadata = sqlalchemy.MetaData()
portal_live = sqlalchemy.Table('portal_live', metadata, autoload_with=database_connection)

check = list(portal_live.columns.keys())

if len(check) > 0:  
    print('Database connection confirmed')
    connection.close() 
else:
    print('Database connection failed')
    exit()

#### Add new full text urls to the database
exec(open('sql_update_fulltexturl.py').read())


#########################################################
################# Data Extraction #######################
#########################################################

# Financial Times
exec(open('auto_FT.py').read())
# Carbon Pulse, E&E News, and S&P Global
exec(open("auto_EE_CarbonPulse_RSS.py").read())
# Springer and Nature Journals
exec(open('auto_Springer_API.py').read())
# Washington Post
exec(open('auto_WP_RSS.py').read())
# Stanford Social Innovation Review
exec(open('auto_SSIR_RSS.py').read())
# New York Times and The New Yorker
exec(open('auto_NYT_NYorker_RSS.py').read())
# The Wall Street Journal
exec(open('auto_WSJ_RSS.py').read())
# MIT Sloan Management Review
exec(open('auto_MIT_RSS.py').read())
# Oil & Gas Journal
#exec(open('auto_OGJ_RSS.py').read())
# LA Times
exec(open('auto_LAT_RSS.py').read())
# Elsevier API: Added 3/13/2023
exec(open('auto_elsevier_API.py').read())
# Times of India RSS feed: Added 11/7/2023
exec(open('auto_times_india.py').read())
# The Guardian API: Added 12/12/2023
exec(open('auto_guardian_API.py').read())
# Sunday Times Africa RSS feed: Added 1/22/2024
exec(open('auto_SundayTimes_RSS.py').read())
# Rigzone
exec(open('auto_rigzone_RSS.py').read())
# Oil Price.com
exec(open('auto_oilPrice_RSS.py').read())
# Al Jazeera
exec(open('auto_alJazeera_RSS.py').read())


#########################################################
################# Data Aggregation ######################
#########################################################

# Point to data folder
mydir = Path("Data/")

# Create blank dataFrame to load new data
df = pd.DataFrame(columns=['title', 'pubDate', 'url', 'url_full_txt', 'creators', 'description', 'pubName', 'doi', 'journalID'])

# Loop through data folder, appending any Excel files to df
for file in mydir.glob('*.xlsx'):
    df_data = pd.read_excel(file)
    df = pd.concat([df, df_data])

# write out df, 
df.to_excel('news_data_pretag.xlsx')


# apply tags to data
news = func_tagging(df)

# rename columns and subset
news.rename(columns={'Adaptation':'adaptation','Behavior':'behavior', 'Emissions':'emissions', 'Environment':'environment', 
            'Finance':'finance','Geography':'geography','Industry':'industry', 'Intervention':'intervention', 'Policy':'policy', 
            'Sector':'sector', 'Technology':'technology','Theory of Change':'theory','Climate Summits/Conferences':'climate_events', 
                         'Organizational Components':'org_comp', 'tag':'tag_concat', 'value':'tag_score'}, inplace=True)


news = news[[ 'title', 'pubDate', 'url', 'url_full_txt','creators', 'description', 'source','adaptation','behavior', 'emissions', 
             'environment','finance','geography','industry', 'intervention', 'policy', 'sector', 'technology', 'theory',
             'climate_events','org_comp','tag_concat', 'tag_score']]

########### Title Format #############

# remove null titles
news = news[news['title'].notnull()]

# Replace special characters that can create problems when adding full text to SharePoint
news['file_title'] = news['title'].str.replace('[\/<>*"?|]', "", regex=True)
news['file_title'] = news['file_title'].str.replace('[:]', "-", regex=True)
# replace all other special characters with a _ 
#news['file_title'] = news['file_title'].str.replace('[^A-Za-z0-9]+', '_', regex=True)
news['file_title'] = news['file_title'].str[:125].str.strip()

# Drop duplicate titles
news.drop_duplicates("title", inplace=True)

# Remove title from description, if necessary
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
news['description'] = news.apply(clean_description, axis=1)

# trim whitespace and "." from beginning of description
news['description'] = news['description'].str.lstrip('.')
news['description'] = news['description'].str.lstrip()

#Trim description and creator fields to match 5000 and 1000 char limits
news['title'] = news['title'].str[:498]
# trim whitespace from title
news['title'] = news['title'].str.strip()
news['description'] = news['description'].str[:4998]
news['creators'] = news['creators'].str[:998]

# add request url for full text
request_app = 'https://apps.powerapps.com/play/e/default-8ed8a585-d8e6-4b00-b9cc-d370783559f6/a/1e7fab62-974b-4a8c-8d40-b002ba18a5a9?art='

# encode file_title
news['safe_string'] = news['file_title'].apply(urllib.parse.quote_plus)


# concat url to file_title and encode
news['url_request'] = request_app + news['safe_string']

# drop safe_string
news.drop(columns=['safe_string'], inplace=True)

# set newsroom = 0 for records with source from rigzone, oilprice, or aljazeera
exclude = ['Rigzone', 'Oil Price.com', 'Al Jazeera']

for index, row in news.iterrows():
    if row['source'] in exclude:
        news.at[index, 'newsroom'] = 0
    else:
        news.at[index, 'newsroom'] = 1

print(news.columns)
news.to_excel('news_data.xlsx')


#########################################################
################# Data Import ###########################
#########################################################
exec(open('sql_import.py').read())

########################################################
############ Import New Manual Submissions #############
########################################################

exec(open('sql_import_fulltext.py').read())

#########################################################
####### Query all with tags and Write to SharePoint #####
## Need to update once we start adding full text to make sure it only pulls full text
#########################################################
# exec(open('sql_export.py').read())

#########################################################
####### Transform Tags to Long Format in Database ######
######## Note: Used for metrics dashboard only ##########
#########################################################

#exec(open('sql_tag_transform.py').read())
tag_transform()

#####################################################################################
#### Update Data Connection in Available Resources Excel File #######################
#### Used instead of the section above because file needs to stay in table format ###
#####################################################################################


# Start an instance of Excel
xlapp = win32com.client.DispatchEx("Excel.Application")

# Open the workbook in said instance of Excel
wb = xlapp.workbooks.open('C:/Users/ghoffman/OneDrive - RMI/Knowledge Resources/Available Resources.xlsx')


# Refresh all data connections.
wb.RefreshAll()
#time.sleep(10)
xlapp.CalculateUntilAsyncQueriesDone()
xlapp.DisplayAlerts = False
wb.Save()
wb.Close(True)
del(wb)
# Quit
xlapp.Quit()
# force quit excel
os.system("taskkill /f /im excel.exe")


# Close connections
database_connection.dispose()
connection.close()


# delete all .json files in the Data folder
mydir = Path("tempdata/articles")

for file in mydir.glob('*.json'):
    os.remove(file)
