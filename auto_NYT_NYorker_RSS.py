# Title: New York Times and The New Yorker RSS Feed Data Extraction
# Purpose: Extract NYT and New Yorker RSS Feed data for the NYT home page, Business Environment, and Climate pages, along with the
# News and Technology feeds for The New Yorker.

# Load Packages
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup

# Identify RSS URLs and loop through them, extracting relevant information from each

rss_url= ['https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', 'https://rss.nytimes.com/services/xml/rss/nyt/EnergyEnvironment.xml', 'https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml',
'https://www.newyorker.com/feed/news', 'https://www.newyorker.com/feed/tech']

df = pd.DataFrame(columns = ['title', 'link', 'author', 'published', 'summary', 'source'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news['source'] = i
    df_news_trim= df_news[['title', 'link', 'author', 'published', 'summary', 'source']]
    df = pd.concat([df, df_news_trim])

# Wrangle variable formats and subset to standard variables
df['source'] = df['source'].str.replace('https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', 'New York Times', regex=True)
df['source'] = df['source'].str.replace('https://rss.nytimes.com/services/xml/rss/nyt/EnergyEnvironment.xml', 'New York Times', regex=True)
df['source'] = df['source'].str.replace('https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml','New York Times', regex=True)
df['source'] = df['source'].str.replace('https://www.newyorker.com/feed/news','The New Yorker', regex=True)
df['source'] = df['source'].str.replace('https://www.newyorker.com/feed/tech','The New Yorker', regex=True)
df['pubDate'] = pd.to_datetime(df['published'], errors='coerce')
df['pubDate']= df['pubDate'].dt.date
df['description'] = df['summary']
df['url'] = df['link']
df['creators'] = df['author']
df_trim= df[['title', 'url', 'pubDate', 'creators', 'description', 'source']]

# Write out
df_trim.to_excel('Data/NYT_NYorker_data.xlsx')

print('New York Times and The New Yorker RSS Feed Data Extraction Complete: ' + str(len(df_trim)) + 'articles extracted')