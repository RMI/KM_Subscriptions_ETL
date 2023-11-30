### News Aggregator Data Pull

# The following script executes extractions for various news and other publication sources. Errors will be raised if an individual script fails. If this occurs, open the individual script to debug.
# If all is running well, you should only need to execute this script when updating the News Aggregator

import pandas as pd
from pathlib import Path
import numpy as np
import re
import win32com.client
import time


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


#########################################################
################# Data Aggregation ######################
#########################################################

# Point to data folder
mydir = Path("Data/")

# Create blank dataFrame to load new data
df = pd.DataFrame(columns=['title', 'pubDate', 'url', 'creators', 'description', 'pubName', 'doi', 'journalID'])

# Loop through data folder, appending any Excel files to df
for file in mydir.glob('*.xlsx'):
    df_data = pd.read_excel(file)
    df = pd.concat([df, df_data])

# write out df, 
df.to_excel('news_data_pretag.xlsx')

#########################################################
################# Data Tagging ##########################
#########################################################

tag_ref = pd.read_excel('tags.xlsx',  index_col="ID", dtype= str)
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

##################################################33
##############################################################

# Define string matching function
def get_matching_values(row, keywords):
    matching_values = {keywords_to_tag[keyword] for keyword in keywords if row.lower().find(keyword) != -1}
    return ','.join(matching_values) if matching_values else ''

# Loop through descriptions, stringing together all matching tags
for i in tag_ref['tag_cat']:
    keywords_tag = tag_ref[tag_ref['tag_cat'] == i]
    keywords_tag = keywords_tag[['tag', 'phrase']]
    keywords_tag.rename(columns={"phrase":"keyword"}, inplace=True)
    keywords_to_tag = keywords_tag.set_index('keyword')['tag'].to_dict()
    news[i] = news['desc_match'].apply(get_matching_values, keywords= keywords_to_tag.keys())

# Create concatenated tag variable
news['tag'] = news[['Adaptation', 'Behavior', 'Emissions', 'Environment', 'Finance','Geography', 'Industry' ,'Intervention',
                         'Policy', 'Sector', 'Technology', 'Theory of Change']].fillna('').agg(','.join, axis=1)

### Create match score variable
# Create id for unique article
news['uid'] = np.arange(0,len(news),1)

# Transform tags to long format 
score_sub = news[['uid','Adaptation', 'Behavior', 'Emissions', 'Environment', 'Finance','Geography', 'Industry' ,'Intervention',
                         'Policy', 'Sector', 'Technology', 'Theory of Change']]
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

news.rename(columns={'Adaptation':'adaptation','Behavior':'behavior', 'Emissions':'emissions', 'Environment':'environment', 
            'Finance':'finance','Geography':'geography','Industry':'industry', 'Intervention':'intervention', 'Policy':'policy', 
            'Sector':'sector', 'Technology':'technology','Theory of Change':'theory', 'tag':'tag_concat', 'value':'tag_score'}, inplace=True)


news = news[[ 'title', 'pubDate', 'url', 'creators', 'description', 'source','adaptation','behavior', 'emissions', 
             'environment','finance','geography','industry', 'intervention', 'policy', 'sector', 'technology', 'theory',
             'tag_concat', 'tag_score']]

########### Title Format #############

# remove null titles
news = news[news['title'].notnull()]

# Replace special characters that can create problems when adding full text to SharePoint
news['file_title'] = news['title'].str.replace('[\/<>*"?|]', "", regex=True)
news['file_title'] = news['file_title'].str.replace('[:]', "-", regex=True)
news['file_title'] = news['file_title'].str[:125]

# Drop duplicate titles
news.drop_duplicates("title", inplace=True)

#Trim description and creator fields to match 5000 and 1000 char limits
news['title'] = news['title'].str[:498]
news['description'] = news['description'].str[:4998]
news['creators'] = news['creators'].str[:998]

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

exec(open('sql_tag_transform.py').read())

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
wb.Close()
# Quit
xlapp.Quit()

