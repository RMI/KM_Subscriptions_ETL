# Title: Google News RSS Feed Data Extraction
# Purpose: Extract Google News RSS Feed data for relevant key words: ["environment", "energy", "sustainability", 
# "climate", "renewable", "clean energy", "carbon", "green technology","emissions"]

# Load Packages
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dateparser
import time
import random
from datetime import date

# Define search terms and urls
keywords = ["environment", "energy", "sustainability", "climate", "renewable", "clean energy", "carbon", "green technology","emissions"]
base_url = "https://www.google.com/search?q={}&gl=us&tbm=nws&num=100"
urls = [base_url.format(keyword.replace(' ', '+')) for keyword in keywords]

# Define function to extract data
def getNewsData(target_urls):

    news_results = []
    for i in target_urls:
        time.sleep(random.randit(1,5))
        headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
        }
        response = requests.get(
        i, headers=headers
        )
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

    output = json.dumps(news_results, indent=2)
    data_dict = json.loads(output)
    df = pd.DataFrame(data_dict)
    print('Google News RSS Feed Data Extraction Complete: ' + str(len(df)) + ' articles extracted')
    return df


df_news = getNewsData(target_urls=urls)

# Wrangle variables and subset to standard variables
df_news['description'] = df_news['snippet']
df_news['url'] = df_news['link']

# Adjust date format
# If date includes 'LIVE', replace with today's date
df_news['date'] = df_news['date'].str.replace('LIVE', str(date.today()))
df_news['date'] = df_news['date'].apply(dateparser.parse)
df_news['pubDate'] = pd.to_datetime(df_news['date'])
df_news['pubDate'] = df_news['pubDate'].dt.date
df_news_trim = df_news[['title', 'url', 'pubDate', 'description', 'source']]

# Remove duplicates based on 'title'
df_news_trim = df_news_trim.drop_duplicates(subset='title')

# Write out
df_news_trim.to_excel('Data/GoogNews_data.xlsx')

