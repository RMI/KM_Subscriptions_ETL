### News Aggregator Data Pull

# The following script executes extractions for various news and other publication sources. Errors will be raised if an individual script fails. If this occurs, open the individual script to debug.
# If all is running well, you should only need to execute this script when updating the News Aggregator

import pandas as pd
from pathlib import Path
#import numpy as np
#import re
import win32com.client
#import time
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
from func_tagging import func_tagging
from func_tag_transform import tag_transform

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
news['file_title'] = news['file_title'].str[:125].str.strip()

# Drop duplicate titles
news.drop_duplicates("title", inplace=True)

#Trim description and creator fields to match 5000 and 1000 char limits
news['title'] = news['title'].str[:498]
# trim whitespace from title
news['title'] = news['title'].str.strip()
news['description'] = news['description'].str[:4998]
news['creators'] = news['creators'].str[:998]

# add request url for full text
news['url_request'] = 'https://apps.powerapps.com/play/e/default-8ed8a585-d8e6-4b00-b9cc-d370783559f6/a/1e7fab62-974b-4a8c-8d40-b002ba18a5a9?art=' + news['file_title']


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


# delete all .json files in the Data folder
mydir = Path("tempdata/articles")

for file in mydir.glob('*.json'):
    os.remove(file)
