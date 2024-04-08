# Title: LA Times RSS Feed Data Extraction
# Purpose: Extract and export news headlines from the Los Angelos Times for the RMI Knowledge Subscriptions Portal

# Load Packages
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup

# Add RSS url
rss_url= ['https://www.latimes.com/world-nation/rss2.0.xml', 'https://www.latimes.com/science/rss2.0.xml', 
        'https://www.latimes.com/politics/rss2.0.xml', 'https://www.latimes.com/environment/rss2.0.xml']

# Parse RSS data and subset relevant variables
df = pd.DataFrame(columns = ['title', 'link', 'summary', 'author' ,'published'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'link', 'summary', 'author', 'published']]
    df = pd.concat([df, df_news_trim])

# Format fields and rename variables to match database
df['source'] = 'LA Times'
df['creators'] = df['author']
df['pubDate'] = pd.to_datetime(df['published'])
df['pubDate']= df['pubDate'].dt.date
p_sep = "</p>"
p_str = '<p>'
df['summary'] = df['summary'].str.replace(p_sep, '', regex=False)
df['summary'] = df['summary'].str.replace(p_str, '', regex=False)
df['summary'] = df['summary'].str.strip()
df['summary'] = df['title'] + '. ' + df['summary'] 
df.rename(columns={'link':'url', 'summary':'description'},inplace=True)
df = df[['title', 'url', 'description', 'creators', 'pubDate', 'source']]

#remove duplicate titles
df.drop_duplicates(subset=['title'], inplace=True)

# write out temp data file
df.to_excel('Data/LAT_data.xlsx')

print('LA Times RSS Feed Data Extraction Complete: ' + str(len(df)) + ' articles extracted')