# Title: Wall Street Journal RSS Feed Data Extraction
# Purpose: Extract and export news headlines from the Wall St. Journal for the RMI Knowledge Subscriptions Portal

# Load Packages
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup

# Add RSS url
rss_url= ['https://feeds.a.dj.com/rss/RSSMarketsMain.xml', 'https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml']

# URLs not currently supported
#'https://feeds.a.dj.com/rss/RSSWorldNews.xml', 'https://feeds.a.dj.com/rss/RSSWSJD.xml'


# Parse RSS data and subset relevant variables
df = pd.DataFrame(columns = ['title', 'link', 'summary',  'published'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'link', 'summary', 'published']]
    df = pd.concat([df, df_news_trim])

# Format fields and rename variables to match database
df['source'] = 'Wall St. Journal'
df['creators'] = ''
df['pubDate'] = pd.to_datetime(df['published'])
df['pubDate']= df['pubDate'].dt.date
df.rename(columns={'link':'url', 'summary':'description'},inplace=True)

# drop duplicate titles
df.drop_duplicates(subset=['title'], inplace=True)

# write out temp data file
df.to_excel('Data/WSJ_data.xlsx')

print('Wall St. Journal RSS Feed Data Extraction Complete: ' + str(len(df)) + ' articles extracted')