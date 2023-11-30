#### Sage Journals API
# Purpose: Extract journal information from Cross Ref API, including Sage Journals


import pandas as pd
from datetime import date, timedelta
import time
import os
from dotenv import load_dotenv
from crossref.restful import Works, Etiquette


load_dotenv('cred.env')
CR_API_MAILTO = os.getenv('CR_API_MAILTO')

cr = Crossref()

# ids here is the Crossref Member ID; 179 = Sage Journals
cr.members(ids = 179, works = True)

url = "http://api.crossref.org/members/179/works?filter=from-pub-date:2023-08-01&rows=5&mailto=ghoffman@rmi.org"
response = re.get(url)
a = response.json()
df = pd.json_normalize(a)
df = pd.DataFrame(a)
print(a)
print(df.head())
response.headers

print(response)

df = pd.DataFrame(a)

print(df.head())
response.content




my_etiquette = Etiquette('ghoffman@rmi.org')


works = Works(etiquette=my_etiquette)

works.CURSOR_AS_ITER_METHOD

query = works.filter(from_pub_date='2023-08-23',member='179').sample(5).select('title','URL', 'abstract', 'published')

dfs = []
it = []
for item in query:
    df = pd.json_normalize(item)
    dfs.append(df)

result = pd.concat(dfs, ignore_index=True)


dfs = []
for item in query:
    df = pd.DataFrame.from_dict(item)
    dfs.append(df)

result = pd.DataFrame(dfs)

print(result.head())
import re
from datetime import datetime

[[2023, 8, 24]]

text = result[['published.date-parts']]
text.replace(',','', regex=False, inplace=True)
text.replace(']','', inplace=True)

result['pub'] = result['published.date-parts'].to_string()

result['date'] = datetime.strptime(result['pub'], '[[%Y, %m, %d]]')
result['pub2'] = result['pub'].str.split('\\n0', expand=False)

print(result['pub'])
match = re.search(r'\d{4}-\d{2}-\d{2}', text)
match = re.search(r'\d{4}-\d{2}-\d{2}', text)

date = datetime.strptime(match.group(), '%Y-%m-%d').date()

result.to_excel('testing_sage.xlsx')


works.FIELDS_SELECT

query.url


## Initialize client
client = ElsClient(API)


# Date parameter to extract articles from current or past year
date = date.today() - timedelta(days=365)
date_tom = date.today() + timedelta(days=1)
date_past = date.today() - timedelta(days=14)
