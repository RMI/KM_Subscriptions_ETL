# Title: Google News RSS Feed Data Extraction
# Purpose: Extract Google News RSS Feed data for relevant key words: ["environment", "energy", "sustainability", 
# "climate", "renewable", "clean energy", "carbon", "green technology","emissions"]

# Load Packages
import json
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import dateparser
import time



# Initialize the UserAgent object - fake-useragent library generates roating headers to avoid blocks / detection
ua = UserAgent(platforms='pc')
ua_current = ua.chrome

# Use the previously initialized UserAgent object
headers = {
    "User-Agent": ua_current
}

keywords = ["environment", "energy", "sustainability", "climate", "renewable", "clean energy", "carbon", "green technology","emissions"]
base_url = "https://www.google.com/search?q={}&gl=us&tbm=nws&num=100"
urls = [base_url.format(keyword.replace(' ', '+')) for keyword in keywords]

news_results = []

for url in urls:
    time.sleep(2)
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    for el in soup.select("div.SoaBEf"):
        news_results.append(
            {
                "link": el.find("a")["href"],
                "title": el.select_one("div.MBeuO").get_text(),
                "snippet": el.select_one(".GI74Re").get_text(),
                "date": el.select_one(".LfVVr").get_text(),
                "source": el.select_one(".NUnG9d span").get_text()
            }
        )

df_news = pd.json_normalize(news_results)

# Wrangle variables and subset to standard variables
df_news['source'] = 'Google News'
df_news['description'] = df_news['snippet']
df_news['url'] = df_news['link']

# Adjust date format
df_news['date'] = df_news['date'].str.replace('LIVE', '')
df_news['date'] = df_news['date'].apply(dateparser.parse)
df_news['pubDate'] = pd.to_datetime(df_news['date'])
df_news['pubDate'] = df_news['pubDate'].dt.date
df_news_trim = df_news[['title', 'url', 'pubDate', 'description', 'source']]

# Remove duplicates based on 'title'
df_news_trim = df_news_trim.drop_duplicates(subset='title')

# Write out
df_news_trim.to_excel('Data/GoogNews_data.xlsx')

print(df_news_trim.head())
print('Google News RSS Feed Data Extraction Complete: ' + str(len(df_news_trim)) + ' articles extracted')