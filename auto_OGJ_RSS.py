# Title:Washington Post RSS Feed Data Extraction
# Purpose: Extract WP RSS Feed data for their World, National, Politics, and Technology feeds.
# Notes: More general feeds than NYT, so may want to monitor over time to see if we can prioritize certain authors.

# Load Packages
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup

# Identify RSS URLs and loop through them, extracting relevant information from each
rss_url= ['https://feeds.feedburner.com/OilGasJournal-GeneralInterest', 'https://feeds.feedburner.com/OilGasJournal-ExplorationDevelopment'
, 'https://feeds.feedburner.com/OilGasJournal-DrillingProduction' , 'https://feeds.feedburner.com/OilGasJournal-PipelinesTransportation', 
'https://feeds.feedburner.com/OilGasJournal-RefiningProcessing']

df = pd.DataFrame(columns = ['title', 'link', 'published',  'summary', 'source'])


for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news['source'] = i
    df_news_trim= df_news[['title', 'link',  'published',  'summary', 'source']]
    df = pd.concat([df, df_news_trim])

# Wrangle variable formats and subset to standard variables
df['source'] = 'Oil & Gas Journal'
df['pubDate'] = pd.to_datetime(df['published'])
df['pubDate']= df['pubDate'].dt.date
df['description'] = df['summary']
df['url'] = df['link']
df_trim= df[['title', 'url', 'pubDate', 'description', 'source']]
df_trim.drop_duplicates(subset=['title'], inplace=True)

# Write out
df_trim.to_excel('Data/OGJ_data.xlsx')