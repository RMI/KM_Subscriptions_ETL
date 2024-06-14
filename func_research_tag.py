import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from datetime import date
from sqlalchemy import text
import re
from datetime import timedelta


def research_tag():

    file = 'Data/backups/research_tag' + str(date.today()) + '.xlsx'
    ###### PURPOSE ############
    # Identify and tag content with Oil Refining and Current Issues tags

    #Load API credentials
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



    # Get existing ids and tags from database
    with database_connection.connect() as conn:
        result = conn.execute(text("select content_id, tag from portal_content_tags"))
        df_check = pd.DataFrame(result.fetchall())
        df_check.columns = result.keys()


    # Add tag GUID from database
    with database_connection.connect() as conn:
        result = conn.execute(text("select tag, tag_cat, guid from ref_content_tags"))
        df_tag_id = pd.DataFrame(result.fetchall())
        df_tag_id.columns = result.keys()

    # Merge tag GUID to tag data
    df_import_f2 = pd.merge(df_check, df_tag_id, how='left', left_on='tag', right_on='tag')

    # Drop rows with duplicate content_id and tag_guid
    df_import_f2 = df_import_f2.drop_duplicates(subset=['content_id', 'guid'])

    # Assign matching programs from tag profiles in database
    with database_connection.connect() as conn:
        result = conn.execute(text("select cost_center, tag_guid from tag_profiles"))
        df_tag_prog = pd.DataFrame(result.fetchall())
        df_tag_prog.columns = result.keys()

    df_tag_prog = df_tag_prog[df_tag_prog['cost_center'] == 'Oil Refining'] 

    # joing tag_cat from df_tag_id to df_tag_prog
    df_tag_prog = pd.merge(df_tag_prog, df_tag_id, how='left', left_on='tag_guid', right_on='guid')

    df_tag_prog = df_tag_prog.drop(['guid'], axis=1)

    # Merge tag profiles to tag data, keeping all matches
    df_import_match = pd.merge(df_import_f2, df_tag_prog, how='outer', left_on='guid', right_on='tag_guid')

    # drop rows with null cost_center
    df_import_match = df_import_match[df_import_match['cost_center'].notnull()]

    df_import_f2 = df_import_match
   # print(len(df_import_match))

    df_tag_prog = df_tag_prog[df_tag_prog['tag_cat'] != 'Adaptation']

   # print(df_tag_prog['tag_cat'].unique())
# identify content_id with combination of tag_guids
    profile_noConflict = df_tag_prog[df_tag_prog['tag_cat'] != 'Current Issues']['tag_guid'].tolist()

    conflictTags = df_tag_prog[df_tag_prog['tag_cat'] == 'Current Issues']['tag_guid'].tolist()

    # remove tags in combo2 from combo
    # for i in profile_noConflict:
    #     if i in conflictTags:
    #         profile_noConflict.remove(i)

    # identify content with at least one tag from profile and one from current events
    df_import_f2['profileTag'] = df_import_f2['tag_guid'].isin(profile_noConflict)
    df_import_f2['conflictTag'] = df_import_f2['tag_guid'].isin(conflictTags)

    # create a new column to identify content_id with at least one tag from current issues and one from subject matter, grouped by content_id
    df_import_f2['tag_match'] = df_import_f2.groupby('content_id')['profileTag'].transform('any') & df_import_f2.groupby('content_id')['conflictTag'].transform('any')

    
   # print(df_import_f2[df_import_f2['tag_match'] == True].sort_values(by='content_id'))

    oil_intel_all = df_import_f2[df_import_f2['profileTag'] == True]

    # filter to only content_id where tag_match is True
    oil_intel = df_import_f2[df_import_f2['tag_match'] == True]

    # filter to unique content_id
    oil_intel = oil_intel.drop_duplicates(subset=['content_id'])

    oil = oil_intel['content_id'].tolist()

    # drop duplicates and null values
    oil_intel_all = oil_intel_all.drop_duplicates(subset=['content_id'])
    oil_intel_all = oil_intel_all[oil_intel_all['content_id'].notnull()]

    oil_all = oil_intel_all['content_id'].tolist()

    # get title, source, content_id, and tag_contact from database for content_id in oil_intel
    df = pd.DataFrame()

    with database_connection.connect() as conn:
        for i in oil:
            result = conn.execute(text("select id, title, source, pubDate, description,  tag_concat, url_full_txt, url_request from portal_live where id =" + str(i)))
            intel = pd.DataFrame(result.fetchall())
            intel.columns = result.keys()
            df = pd.concat([df, intel], axis=0)

    # reduce description to 500 characters
    df['description'] = df['description'].str[:500] + '...'

    df.to_excel('Data/Research/oil_intel' + str(date.today()) + '.xlsx')

        # Where there is a match, create a new column with the matching tags, grouped by content_id
        #df_import_f2['matching_tags'] = df_import_f2[df_import_f2['combo_match'] == True].groupby('content_id')['tag'].transform(lambda x: ', '.join(x))

# Debug this on Monday

    # get title, source, content_id, and tag_contact from database for content_id in oil_intel_all

    with database_connection.connect() as conn:
            result = conn.execute(text("select id, title, source, pubDate, description, tag_concat, tag_score, url_full_txt, url_request from portal_live where tag_score > 1 and pubDate >" + str(date.today() - timedelta(days=90))))
            all_articles = result.fetchall()
            all_articles = pd.DataFrame(all_articles)
            all_articles.columns = result.keys()

    # subset all_articles to content_id in oil_all
    df = all_articles[all_articles['id'].isin(oil_all)]

    # reduce description to 500 characters
    df['description'] = df['description'].str[:500] + '...'

    # filter to tag score greater than 1
    df = df[df['tag_score'] > 1]
    print(len(df))

    df.to_excel('Data/Research/oil_intel_all' + str(date.today()) + '.xlsx')
        #print(df_import_f2[df_import_f2['combo_match'] == True].sort_values(by='content_id'))
    
    # print rows from each dataframe
    print(str(len(oil_intel)) + ' rows in oil_intel')
    print(str(len(oil_intel_all)) + ' rows in oil_intel_all')

    # Return the dataframes
    return oil_intel, oil_intel_all
