# Title:Washington Post RSS Feed Data Extraction
# Purpose: Extract WP RSS Feed data for their World, National, Politics, and Technology feeds.
# Notes: More general feeds than NYT, so may want to monitor over time to see if we can prioritize certain authors.

# Load Packages
import pandas as pd
import feedparser
from random import randint
from time import sleep


# Identify RSS URLs and loop through them, extracting relevant information from each

rss_url= ['https://www.washingtonpost.com/arcio/rss/category/opinions/?itid=lk_inline_manual_7', 'https://feeds.washingtonpost.com/rss/world?itid=lk_inline_manual_35', 
        'https://feeds.washingtonpost.com/rss/business/technology?itid=lk_inline_manual_30', 'https://www.washingtonpost.com/arcio/rss/category/politics/?itid=lk_inline_manual_2',
        'https://feeds.washingtonpost.com/rss/national?itid=lk_inline_manual_31']

df = pd.DataFrame(columns = ['title', 'link', 'author', 'published', 'summary'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'link', 'author', 'published', 'summary']]
    df = pd.concat([df, df_news_trim])
    sleep(randint(2,6))
    
# Wrangle variable formats and subset to standard variables
df['source'] = 'Washington Post'
df['pubDate'] = df['published'].str[:16]
df['pubDate'] = pd.to_datetime(df['pubDate'], infer_datetime_format=True, exact=False, utc=True)
df['pubDate']= df['pubDate'].dt.date
df['description'] = df['summary']
df['url'] = df['link']
df['creators'] = df['author']
df_trim= df[['title', 'url', 'pubDate', 'creators', 'description', 'source']]
df_trim.drop_duplicates(subset=['title'], inplace=True)

# Write out
df_trim.to_excel('Data/WP_data.xlsx')

print('Washington Post RSS Feed Data Extraction Complete: ' + str(len(df_trim)) + ' articles extracted')