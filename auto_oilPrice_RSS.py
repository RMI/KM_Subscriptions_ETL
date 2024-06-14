# Title:Rigzone RSS Feed Data Extraction
# Purpose: Extract Rigzone RSS Feed data for all active feeds.
# Notes: Content intended for oil refining intel only, not newsroom

# Load Packages
import pandas as pd
import feedparser
from random import randint
from time import sleep

rss_url = ['https://oilprice.com/rss/main']

df = pd.DataFrame(columns = ['title', 'link', 'published', 'summary'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'link', 'published','summary']]
    df = pd.concat([df, df_news_trim])
    sleep(randint(2,6))

# Wrangle variable formats and subset to standard variables
df['source'] = 'Oil Price.com'
df['pubDate'] = df['published'].str[:16]
df['pubDate'] = pd.to_datetime(df['pubDate'], infer_datetime_format=True, exact=False, utc=True)
df['pubDate']= df['pubDate'].dt.date
df['description'] = df['summary']
df['url'] = df['link']
df_trim= df[['title', 'url', 'pubDate', 'description', 'source']]
df_trim.drop_duplicates(subset=['title'], inplace=True)


# Write out
df_trim.to_excel('Data/oilprice_data.xlsx')

print('Oil Price.com RSS Feed Data Extraction Complete: ' + str(len(df_trim)) + ' articles extracted')