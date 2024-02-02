#### Elsevier SCOPUS Journal API
# Purpose: Extract journal information from SCOPUS API, managed by Elsevier
#          Used as an alternative to Science Direct API due to lack of affiliation token
# Notes: 
    # May need to expand criteria, currently pulls articles published during current year with keywords "climate change" and "renewable energy"
    # Could try "Environment", but that is likely to get pretty broad
# Reference:
    # https://github.com/ElsevierDev/elsapy
    # https://dev.elsevier.com/api_docs.html
    # https://dev.elsevier.com/tips/ScienceDirectSearchTips.htm 

# Load packages
from elsapy.elsclient import ElsClient
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import pandas as pd
from datetime import date, timedelta
import time
import os
from dotenv import load_dotenv
    

#Load API credentials
load_dotenv('cred.env')
API = os.getenv('ELSEVIER_API_LIB')

## Initialize client
client = ElsClient(API)


# Date parameter to extract articles from current or past year
date = date.today() - timedelta(days=365)
date_tom = date.today() + timedelta(days=1)
date_past = date.today() - timedelta(days=14)

# Selecting journal subject domains
journ = ['DECI', 'EART', 'ECON', 'ENER', 'ENGI', 'ENVI', 'MATE', 'SOCI']

# Loop through domains above, extracting results for all articles with renewable energy or climate change in the author keywords
result_list = []
for i in journ:
    subj = "SUBJAREA("+i+")"
    query_string = '""AUTHKEY({renewable energy} OR {climate change}) AND '+ subj + ' AND ORIG-LOAD-DATE > '+date_past.strftime(format='%Y%m%d')+' AND ORIG-LOAD-DATE < '+date_tom.strftime(format='%Y%m%d')+' AND PUBYEAR > '+ date.strftime(format = '%Y') +'""'
   # query_string = '""AUTHKEY({renewable energy} OR {climate change}) AND '+ subj + ' AND PUBYEAR > '+ date.strftime(format = '%Y') +'""'
    doc_srch = ElsSearch(query_string,'scopus')
    doc_srch.execute(client, get_all = True)
    df_result = doc_srch.results_df
    df_result = pd.DataFrame(df_result)
    df_result = df_result[['dc:title','dc:creator','prism:publicationName',
                          'prism:coverDate','subtypeDescription','prism:doi','openaccessFlag']]
    result_list.append(df_result)
    time.sleep(2)

df_entries = pd.concat(result_list)
# Drop duplicate results
df_entries.drop_duplicates("prism:doi", inplace=True)

df_entries = df_entries[df_entries['prism:coverDate'] < date_tom.strftime(format = '%Y-%m-%d')]


# Clean up variable names and write out data
df_entries['source'] = 'Elsevier Journals'
df_entries['description'] = df_entries['dc:title']
df_entries['url'] = 'http://dx.doi.org/' + df_entries['prism:doi']

df_entries.rename(columns={'dc:creator':'creators', 'prism:publicationName':'pubName',
                            'dc:title':'title', 'prism:coverDate':'pubDate', 'prism:doi':'doi',
                            'publicationName': 'pubName'}, inplace=True)

df_entries = df_entries[['source', 'url', 'title', 'creators', 'doi', 'pubName', 'pubDate', 'description']]

df_entries.to_excel('Data/elsevier_data.xlsx')

print('Elsevier Journal Data Extraction Complete: ' + str(len(df_entries)) + ' articles extracted')
