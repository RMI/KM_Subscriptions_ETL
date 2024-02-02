# Title: Sunday Times Africa RSS Feed Data Extraction
# Purpose: Extract and export news headlines from the Los Angelos Times for the RMI Knowledge Subscriptions Portal

# Load Packages
import pandas as pd
import feedparser

# Add RSS url
rss_url= ['https://www.timeslive.co.za/rss/?section=news', 'https://www.timeslive.co.za/rss/?section=politics', 
        'https://www.timeslive.co.za/rss/?publication=sunday-times&section=news', 'https://www.timeslive.co.za/rss/?publication=sunday-times&section=investigations',
        'https://www.timeslive.co.za/rss/?publication=sunday-times&section=opinion-and-analysis']

# Parse RSS data and subset relevant variables
df = pd.DataFrame(columns = ['title', 'link', 'summary','published'])

for i in rss_url:
    
    news_feed = feedparser.parse(i) 
    df_news=pd.json_normalize(news_feed.entries)
    df_news_trim= df_news[['title', 'link', 'summary', 'published']]
    df = pd.concat([df, df_news_trim])

# remove articles with these strings in the title
drops = ['WATCH | ', 'LISTEN | ', 'RECORDED | ', 'PODCAST | ', 'LIVE | ', 'LIVE STREAM | ', 'CARTOON |']

for i in drops:
    df = df.drop(df[df['title'].str.contains(i, regex=False)].index)

# Remove artile type from title, e.g. 'WATCH | ' or 'LISTEN | '
mask = df['title'].str.contains('|', regex=False)
df.loc[mask, 'title'] = df.loc[mask, 'title'].str.split('|').str[1]

df['title'] = df['title'].str.strip()

# Format fields and rename variables to match database
df['source'] = 'Sunday Times Africa'
df['pubDate'] = pd.to_datetime(df['published'])
df['pubDate']= df['pubDate'].dt.date
df['summary'] = df['title'] + '. ' + df['summary']
p_sep = "</p>"
p_str = '<p>'
df['summary'] = df['summary'].str.replace(p_sep, '', regex=False)
df['summary'] = df['summary'].str.replace(p_str, '', regex=False)
df.rename(columns={'link':'url', 'summary':'description'},inplace=True)
df = df[['title', 'url', 'description', 'pubDate', 'source']]

#remove duplicate titles
df.drop_duplicates(subset=['title'], inplace=True)

# write out temp data file
df.to_excel('Data/ST_data.xlsx')

print('Sunday Times RSS Feed Data Extraction Complete: ' + str(len(df)) + ' articles extracted')