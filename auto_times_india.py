# Import packages
import pandas as pd
import feedparser

# URLs for rss feeds that have consistent fields present. Includes India and Environment sections of Times of India
rss_url= ['https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms' ,'https://timesofindia.indiatimes.com/rssfeeds/2647163.cms']

# Empty df to append into later
df = pd.DataFrame(columns = ['title','summary', 'link',  'published'])

# Loop through URLs, extract relevant fields and append to df
for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'summary', 'link', 'published']]
    df = pd.concat([df, df_news_trim])

# Trim summary to remove href
sep = r'/></a>'
df['summary'] = df['summary'].str.split(sep, regex=True).str[1]

# add source
df['source'] = 'Times of India'

# fix dates
df['published'] = pd.to_datetime(df['published'])
df['published']= df['published'].dt.date

# rename to match database
df.rename(columns={'link':'url', 'published':'pubDate', 'summary':'description'},inplace=True)

#write out
df.to_excel('Data/timesOfIndia_data.xlsx')