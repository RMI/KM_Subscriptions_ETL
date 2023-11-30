import pandas as pd
from pathlib import Path
import numpy as np


tag_ref = pd.read_excel('tags.xlsx',  index_col="ID", dtype= str)
tag_ref['phrase'] = tag_ref.phrase.str.lower()



def get_matching_values(row, keywords):
    matching_values = {keywords_to_tag[keyword] for keyword in keywords if row.lower().find(keyword) != -1}
    return ','.join(matching_values) if matching_values else ''

# Loop through descriptions, stringing together all matching tags

def keyword_tag(news, tag_ref):

    tag_ref = tag_ref
    tag_ref['phrase'] = tag_ref.phrase.str.lower()


    for i in tag_ref['tag_cat']:
        keywords_tag = tag_ref[tag_ref['tag_cat'] == i]
        keywords_tag = keywords_tag[['tag', 'phrase']]
        keywords_tag.rename(columns={"phrase":"keyword"}, inplace=True)
        keywords_to_tag = keywords_tag.set_index('keyword')['tag'].to_dict()
        news[i] = news['desc_match'].apply(get_matching_values, keywords= keywords_to_tag.keys())

# Create concatenated tag variable
    news['tag'] = news[['Behavior', 'Emissions', 'Environment', 'Industry' ,'Intervention',
                            'Policy', 'Region', 'Status', 'Sector', 'Technology']].fillna('').agg(','.join, axis=1)