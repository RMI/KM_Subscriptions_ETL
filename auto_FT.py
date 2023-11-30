# Title: Financial Times News Feed Extraction
# Purpose: Open and scrape title and teaser information from the four most recent pages on the Financial Times news feed
# Notes: publicationDate is currently defaulted to today's date. URL is also defaulted to the news feed page, both due to 
#        complications with the html of the news feed page.

# Load Packages
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from time import sleep
from random import randint
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from datetime import date
from webdriver_auto_update.webdriver_auto_update import WebdriverAutoUpdate

# Create empty lists
data_desc=[]
data_title=[]
#data_meta=[]
# data_link=[]
pages = np.arange(1, 5, 1)

# Target directory to store chromedriver
driver_directory = "C:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/KM_Subscription_ETL_Pipelines/"
# Create an instance of WebdriverAutoUpdate
driver_manager = WebdriverAutoUpdate(driver_directory)
# Call the main method to manage chromedriver
driver_manager.main()

from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# Open each of the first 4 pages of FT News Page, extract the html, and extract the relevant information
for page in pages:
    
    page= "https://www.ft.com/news-feed?page=" + str(page)
   # driver = webdriver.Edge(executable_path="C:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/KM_Subscription_ETL_Pipelines/msedgedriver.exe")
   # driver = webdriver.Chrome(executable_path="C:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/KM_Subscription_ETL_Pipelines/chromedriver.exe")
    driver.get(page)  
    sleep(randint(2,10))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    for tag in soup.select('div.o-teaser--live-blog-package'):
        tag.decompose()
    desc_table = soup.find_all(class_=['o-teaser__standfirst'])
    title_table = soup.find_all(class_=['o-teaser__heading'])
  #  meta_table = soup.find_all(class_=['o-teaser__meta'])
    for tag in desc_table:
        data_desc.append(tag.get_text())
    for tag in title_table:
        data_title.append(tag.get_text())

sleep(2)

# Create dataframes for each of the components above, then concatenate into a single dataframe.
# Though inefficient, the multi-page layout of the news feed made this the only viable option.
df_desc = pd.DataFrame(data_desc)
df_desc.columns = ['description']
df_desc = df_desc[~df_desc['description'].str.contains('FT Crossword')]
df_desc = df_desc[~df_desc['description'].str.contains('Quiz')]
df_desc['description'].replace("", pd.NA, inplace=True)
df_desc.dropna(subset=['description'], inplace=True)
df_desc.reset_index(drop=True, inplace=True)
df_title = pd.DataFrame(data_title)
df_title.columns = ['title']
df_title = df_title[~df_title['title'].str.contains('FT Crossword')]
df_title = df_title[~df_title['title'].str.contains('Quiz')]
df_title['title'].replace("", pd.NA, inplace=True)
df_title.dropna(subset=['title'], inplace=True)
df_title.reset_index(drop=True, inplace=True)
#df_meta = pd.DataFrame(data_meta)
#df_meta.columns = ['meta']

df = pd.concat([df_title, df_desc], axis=1)
df.columns = ['title','description']

# Add default date and url values and drop any duplicate titles
today = date.today()
df['pubDate'] = today
df['source'] = 'Financial Times'
df['url'] = 'https://www.ft.com/news-feed'
df.drop_duplicates(subset=['title'], inplace=True)
df = df[~df['title'].str.contains('Letter: ')]

df.to_excel('Data/ft_data.xlsx')
