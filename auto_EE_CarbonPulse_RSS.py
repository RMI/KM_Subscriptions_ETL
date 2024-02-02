# Import packages
import pandas as pd
import requests
import feedparser

# URLs for rss feeds that have consistent fields present
rss_url= ['https://www.eenews.net/articles/feed/', 'https://carbon-pulse.com/feed/', 'https://www.capitaliq.spglobal.com/SPGMI.Services.RSSFeed.Service/RSSFeed/GetFeed/203C04DD-7C18-49EF-B895-80BD1DEF1341']

# url removed on 3/16 due to sub cancel ,

# Empty df to append into later
df = pd.DataFrame(columns = ['title', 'link', 'author', 'published', 'summary', 'source'])

# Loop through URLs, extract relevant fields and append to df
for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news['source'] = i
    df_news_trim= df_news[['title', 'link', 'author', 'published', 'summary', 'source']]
    df = pd.concat([df, df_news_trim])

# Add source based on rss url
df['source'] = df['source'].str.replace('https://www.eenews.net/articles/feed/', 'E&E News', regex=True)
df['source'] = df['source'].str.replace('https://carbon-pulse.com/feed/', 'Carbon Pulse', regex=True)
df['source'] = df['source'].str.replace('https://www.capitaliq.spglobal.com/SPGMI.Services.RSSFeed.Service/RSSFeed/GetFeed/203C04DD-7C18-49EF-B895-80BD1DEF1341', 'S&P Global IQ', regex=True)

# Format date, remove unnecessary strings from summary, and rename columns to match master fields
df['published'] = pd.to_datetime(df['published'])
df['published']= df['published'].dt.date
sep = "</p>"
p_str = '<p>'
img = '<img src="https://www.capitaliq.spglobal.com/interactivex/images/Platform09/SNL_RSS_logo_small.gif" />'
img_2 = 'S&amp;P '
df['summary'] = df['summary'].str.split(sep).str[0]
df['summary'] = df['summary'].str.replace(p_str, '', regex=False)
df['summary'] = df['summary'].str.replace(img, '', regex=False)
df['summary'] = df['summary'].str.replace(img_2, '', regex=False)
df['summary'] = df['title'] + '. ' + df['summary']
df.rename(columns={'link':'url', 'author':'creators', 'published':'pubDate', 'summary':'description'},inplace=True)

# write out
df.to_excel('Data/CarbonPulse_EE_News_S&P_data.xlsx')

print('Carbon Pulse, E&E News, and S&P Global IQ RSS Feed Data Extraction Complete: ' + str(len(df)) + ' articles extracted')