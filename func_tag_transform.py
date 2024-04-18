import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from datetime import date
from sqlalchemy import text
import re

run_type = 'newsroom'

def tag_transform(run_type):

    file = 'Data/backups/tag_import' + str(date.today()) + '.xlsx'
    ###### PURPOSE ############
    # Transform wide, comma separated tag data into long format and import to MySQL.

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

    # Get existing wide tag content
    with database_connection.connect() as conn:
        result = conn.execute(text("select id, adaptation,behavior, emissions, environment,finance,geography,industry, intervention, policy, sector, technology, theory, climate_events, org_comp from portal_live where tag_concat IS NOT NULL"))
        df1 = pd.DataFrame(result.fetchall())
        df1.columns = result.keys()

    # Get existing long format ids
    with database_connection.connect() as conn:
        result = conn.execute(text("select content_id from portal_content_tags"))
        df_check = pd.DataFrame(result.fetchall())
        df_check.columns = result.keys()

    # cross check to filter out wide format that have already been converted
    df = df1[~df1.id.isin(df_check['content_id'])]

    # Transform wide tag categories to long format and remove Null
    df_import = pd.melt(df, id_vars= 'id', value_vars= df[1:len(df.columns)], value_name='tag', var_name='tag_cat')
    df_import = df_import[df_import['tag'].notnull()]
    df_import = df_import[df_import['tag'] != '']

    # Split comma separated tags and join back to id and category
    df_tags = df_import['tag'].str.split(",", expand=True)
    df_import = df_import.drop('tag', axis=1)
    df_import_f = pd.concat([df_import, df_tags], axis=1)

    # Transform wide tags to long format, preserving id and tag category
    df_import_f2 = pd.melt(df_import_f, id_vars= {'id', 'tag_cat'}, value_vars= df_import_f[2:len(df_import_f.columns)], value_name='tag', var_name='tag_count')
    df_import_f2 = df_import_f2[df_import_f2['tag'].notnull()]
    df_import_f2 = df_import_f2[df_import_f2['tag'] != '']
    df_import_f2.rename(columns={'id':'content_id'}, inplace=True)
    df_import_f2 = df_import_f2.drop('tag_count', axis=1)

    # Add tag GUID from database
    with database_connection.connect() as conn:
        result = conn.execute(text("select tag, tag_cat, guid from ref_content_tags"))
        df_tag_id = pd.DataFrame(result.fetchall())
        df_tag_id.columns = result.keys()

    # Merge tag GUID to tag data
    df_import_f2 = pd.merge(df_import_f2, df_tag_id, how='left', left_on='tag', right_on='tag')

    # Drop rows with duplicate content_id and tag_guid
    df_import_f2 = df_import_f2.drop_duplicates(subset=['content_id', 'guid'])

    # Assign matching programs from tag profiles in database
    with database_connection.connect() as conn:
        result = conn.execute(text("select cost_center, tag_guid from tag_profiles"))
        df_tag_prog = pd.DataFrame(result.fetchall())
        df_tag_prog.columns = result.keys()

    # Merge tag profiles to tag data, keeping all matches
    df_import_f2 = pd.merge(df_import_f2, df_tag_prog, how='outer', left_on='guid', right_on='tag_guid')


    if run_type == 'research':

    # identify content_id with combination of tag_guids
        profile_noConflict = df_tag_prog[df_tag_prog['cost_center'] == 'Oil Refining']['tag_guid'].tolist()

        conflictTags = df_tag_id[df_tag_id['tag_cat'] == 'Current Issues']['guid'].tolist()

        # remove tags in combo2 from combo
        for i in profile_noConflict:
            if i in conflictTags:
                profile_noConflict.remove(i)

        # identify content with at least one tag from profile and one from current events
        df_import_f2['profileTag'] = df_import_f2['tag_guid'].isin(profile_noConflict)
        df_import_f2['conflictTag'] = df_import_f2['tag_guid'].isin(conflictTags)

        # create a new column to identify content_id with at least one tag from combo and one from combo2, grouped by content_id
        df_import_f2['tag_match'] = df_import_f2.groupby('content_id')['profileTag'].transform('any') & df_import_f2.groupby('content_id')['conflictTag'].transform('any')


        #print(df_import_f2[df_import_f2['tag_match'] == True].sort_values(by='content_id'))

        # filter to only content_id where tag_match is True
        oil_intel = df_import_f2[df_import_f2['tag_match'] == True]

        # filter to unique content_id
        oil_intel = oil_intel.drop_duplicates(subset=['content_id'])

        oil = oil_intel['content_id'].tolist()

        # get title, source, content_id, and tag_contact from database for content_id in oil_intel
        df = pd.DataFrame()

        with database_connection.connect() as conn:
            for i in oil:
                result = conn.execute(text("select id, title, source, pubDate, tag_concat from portal_live where id =" + str(i)))
                intel = pd.DataFrame(result.fetchall())
                intel.columns = result.keys()
                df = pd.concat([df, intel], axis=0)

        df.to_excel('Data/oil_intel' + str(date.today()) + '.xlsx')

        # Where there is a match, create a new column with the matching tags, grouped by content_id
        #df_import_f2['matching_tags'] = df_import_f2[df_import_f2['combo_match'] == True].groupby('content_id')['tag'].transform(lambda x: ', '.join(x))


        #print(df_import_f2[df_import_f2['combo_match'] == True].sort_values(by='content_id'))



    ###################
    # drop rows with null content_id
    df_import_f2 = df_import_f2[df_import_f2['content_id'].notnull()]

    # drop tag_guid and tag_cat columns
    df_import_f2 = df_import_f2.drop(['tag_guid'], axis=1)

    # rename guid to tag_guid
    df_import_f2.rename(columns={'guid':'tag_guid'}, inplace=True)

    # melt cost_center to comma separated string
    df_import_f2['cost_center'] = df_import_f2['cost_center'].astype(str)
    tag_profiles = df_import_f2.groupby('content_id').agg({'cost_center': ', '.join}).reset_index()

    # remove duplicates from cost_center
    tag_profiles['cost_center'] = tag_profiles['cost_center'].apply(lambda x: ', '.join(set(x.split(', '))))

    # remove nan values
    tag_profiles['cost_center'] = tag_profiles['cost_center'].str.replace('nan', '')

    # remove leading and trailing spaces
    tag_profiles['cost_center'] = tag_profiles['cost_center'].str.strip()
    # where there are two spaces, replace with one
    pattern = re.compile(r'\s{2,}')
    tag_profiles['cost_center'].replace(pattern, ' ', regex = True, inplace = True)
    # Remove duplicate and trailing commas
    pattern = re.compile(r',{2,}')
    tag_profiles['cost_center'].replace(pattern, ',', regex = True, inplace = True)
    pattern = re.compile(r',\s,\s')
    tag_profiles['cost_center'].replace(pattern, ', ', regex = True, inplace = True)
    # where there are three commas, replace with one
    pattern = re.compile(r',{3,}')
    tag_profiles['cost_center'].replace(pattern, ',', regex = True, inplace = True)
    # where there are three commas with space in between each, replace with one
    pattern = re.compile(r',\s{3,}')
    tag_profiles['cost_center'].replace(pattern, ', ', regex = True, inplace = True)
    pattern = re.compile(r',\s,\s,\s')
    tag_profiles['cost_center'].replace(pattern, ', ', regex = True, inplace = True)
    pattern = re.compile(r'(^[,\s]+)|([,\s]+$)')
    tag_profiles['cost_center'].replace(pattern, '', regex = True, inplace = True)

    # drop null cost_center
    tag_profiles = tag_profiles[tag_profiles['cost_center'] != '']

    # drop oil refining cost center
    tag_profiles = tag_profiles[tag_profiles['cost_center'] != 'Oil Refining']


    # change content_id to integer
    tag_profiles['content_id'] = tag_profiles['content_id'].astype(int)

    df_import_f2 = df_import_f2.drop(['cost_center'], axis=1)

    # Write out backup and import to MySQL
    df_import_f2.to_excel(file)

    # drop tag_cat_y and rename tag_cat_x to tag_cat
    df_import_f2 = df_import_f2.drop(['tag_cat_y'], axis=1)
    df_import_f2.rename(columns={'tag_cat_x':'tag_cat'}, inplace=True)


    df_import_f2.to_sql(con=database_connection, name='portal_content_tags', if_exists='append', index=False)

    # update cost_center in portal_live
    with database_connection.connect() as conn:
        for index, row in tag_profiles.iterrows():
            conn.execute(text("update portal_live set profiles = :cost_center where id = :content_id"), {'cost_center' : row['cost_center'], 'content_id' : row['content_id']})
           # conn.commit()

    # Close connections
    database_connection.dispose()
    conn.close()

    # return success or failure message
    if len(df_import_f2) > 0:
        return 'Success'
    else:
        return 'Failure'