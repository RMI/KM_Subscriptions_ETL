# Import packages
import pandas as pd
import requests
import feedparser

# URLs for rss feeds that have consistent fields present
rss_url= 'https://ssir.org/site/rss_2.0'

# Parse RSS feed and create dataFrame
news_feed = feedparser.parse(rss_url) 
df_news=pd.json_normalize(news_feed.entries)

# Wrangle variables and subset to fields for the app
df_news['source']='Stanford Social Innovation Review'
df_news['pubDate'] = pd.to_datetime(df_news['updated'])
df_news['pubDate']= df_news['pubDate'].dt.date
df_news['summary'] = df_news['title'] + '. ' + df_news['summary']
df_news['description'] = df_news['summary']
df_news['url'] = df_news['link']
df_news_trim= df_news[['title', 'url', 'pubDate', 'description', 'source']]

#write out
df_news_trim.to_excel('Data/SSIR_data.xlsx')

print('Stanford Social Innovation Review RSS Feed Data Extraction Complete: ' + str(len(df_news_trim)) + ' articles extracted')