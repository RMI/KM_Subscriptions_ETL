# Title: MIT Sloan Management Review RSS Feed Data Extraction
# Purpose: Extract and export article and report titles and descriptions for the RMI Knowledge Subscriptions Portal

# Load Packages
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup

# Add RSS url
rss_url= 'http://feeds.feedburner.com/mitsmr'

# Parse RSS feed and create dataFrame
news_feed = feedparser.parse(rss_url) 
df_news=pd.json_normalize(news_feed.entries)

df_news=df_news[['title', 'link', 'published','summary']]

# Wrangle variables and subset to fields for the app
df_news['source']='MIT Sloan Management Review'
df_news['pubDate'] = pd.to_datetime(df_news['published'])
df_news['pubDate']= df_news['pubDate'].dt.date
df_news['description'] = df_news['summary']
df_news['url'] = df_news['link']
df_news_trim= df_news[['title', 'url', 'pubDate', 'description', 'source']]

#write out
df_news_trim.to_excel('Data/MIT_data.xlsx')
