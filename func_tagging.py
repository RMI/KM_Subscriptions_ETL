# Purpose: Function to tag news articles with keywords from a reference table. 
# This function is used in the sql_full_table_update.py, auto_compile.py, and sql_import_fulltext.py
# to tag news articles with keywords from a reference table. 
# The function takes a dataframe of news articles as input and returns a dataframe with tags appended to the news articles. 
# The function uses a reference table of keywords and tags to match keywords in the news articles and assign corresponding tags. 
# The function also calculates a match score for each news article based on the number of tags assigned.
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
import os
import numpy as np
import re



def func_tagging(df):

    load_dotenv('cred.env')
    rmi_db = os.getenv('DBASE_PWD')
    rmi_ip = os.getenv('DBASE_IP')

    database_username = 'rmiadmin'
    database_password = rmi_db
    database_ip       = rmi_ip
    database_name     = 'rmi_km_news'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                 format(database_username, database_password, 
                                                         database_ip, database_name))
        
    with database_connection.connect() as conn:
         result = conn.execute(text("select tag_cat, tag, phrase from ref_content_tags where newsroom = 'Yes'"))
         df1 = pd.DataFrame(result.fetchall())
         df1.columns = result.keys()

    tag_ref = df1

    tag_ref['phrase'] = tag_ref.phrase.str.lower()

    news = df


    # # Create a sample dataframe
    # data = {
    #     'title': ['Title1', 'Title2', 'Title3', 'Title4', 'Title5'],
    #     'description': ['Description1', '', 'Description3', 'Description4', 'Description5']
    # }

    # news = pd.DataFrame(data)

    # If description in news is null, fill with title, otherwise concatenate title and description
    # news['description'] = news.apply(lambda row: row['title'] if pd.isnull(row['description']) else row['title'] + '. ' + row['description'], axis=1)
    news['description'] = news.apply(lambda row: row['title'] if pd.isnull(row['description']) else row['description'], axis=1)

    # Generate average description length by source
    desc_len = news[['description', 'source']]
    desc_len['char_len'] = desc_len['description'].str.len()
    len_avg = desc_len.groupby(['source']).mean(numeric_only = True)
    # Average description length across sources
    char_limit = len_avg['char_len'].mean().round()
    char_limit = int(char_limit)
    # create a description field for tag matching in lower case and capped at average description length
    news['desc_match'] = news['description'].str[:char_limit].str.lower()

    ##################################################33
    ##############################################################
    # Define string matching function
    def get_matching_values(row, keywords):
        matching_values = {keywords_to_tag[keyword] for keyword in keywords if row.lower().find(keyword) != -1}
        return ','.join(matching_values) if matching_values else ''

    # Loop through descriptions, stringing together all matching tags
    for i in tag_ref['tag_cat']:
        keywords_tag = tag_ref[tag_ref['tag_cat'] == i]
        keywords_tag = keywords_tag[['tag', 'phrase']]
        keywords_tag.rename(columns={"phrase":"keyword"}, inplace=True)
        keywords_to_tag = keywords_tag.set_index('keyword')['tag'].to_dict()
        news[i] = news['desc_match'].apply(get_matching_values, keywords= keywords_to_tag.keys())

    # Create concatenated tag variable
    news['tag'] = news[['Adaptation', 'Behavior', 'Emissions', 'Environment', 'Finance','Geography', 'Industry' ,'Intervention',
                            'Policy', 'Sector', 'Technology', 'Theory of Change', 'Climate Summits/Conferences', 
                            'Organizational Components']].fillna('').agg(','.join, axis=1)

    ### Create match score variable
    # Create id for unique article
    news['uid'] = np.arange(0,len(news),1)

    # Transform tags to long format 
    score_sub = news[['uid','Adaptation', 'Behavior', 'Emissions', 'Environment', 'Finance','Geography', 'Industry' ,'Intervention',
                            'Policy', 'Sector', 'Technology', 'Theory of Change','Climate Summits/Conferences', 
                            'Organizational Components']]
    score = score_sub.melt(id_vars = ['uid'], ignore_index=False).reset_index()
    score['value'].replace('', np.nan, inplace=True)
    score = score.dropna()
    # Create count of tag categories
    tag_score = score.groupby('uid')['value'].count()
    # Join score back to news df
    news = news.join(tag_score, on='uid')

    # Remove duplicate and trailing commas from null tags
    pattern = re.compile(r',{2,}')
    news['tag'].replace(pattern, ',', regex = True, inplace = True)

    pattern = re.compile(r'(^[,\s]+)|([,\s]+$)')
    news['tag'].replace(pattern, '', regex = True, inplace = True)

    return news