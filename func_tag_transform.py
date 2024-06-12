import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from datetime import date
from sqlalchemy import text
import re


def tag_transform():

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

    # filter out rows in df1 that are already in df_check
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
    df_import_f2 = df_import_f2.drop(columns={'tag_cat','tag_count'}, axis=1)

    # Add tag GUID from database
    with database_connection.connect() as conn:
        result = conn.execute(text("select tag, tag_cat, guid from ref_content_tags"))
        df_tag_id = pd.DataFrame(result.fetchall())
        df_tag_id.columns = result.keys()

    # Merge tag GUID to tag data
    df_import_f2 = pd.merge(df_import_f2, df_tag_id, how='left', left_on='tag', right_on='tag')

    # print number of tags that do not have a GUID
    print('Number of tags without a GUID: ', len(df_import_f2[df_import_f2['guid'].isnull()]))
   
    # Drop rows with duplicate content_id and tag_guid
    df_import_f2 = df_import_f2.drop_duplicates(subset=['content_id', 'guid'])

    # Assign matching programs from tag profiles in database
    with database_connection.connect() as conn:
        result = conn.execute(text("select cost_center, tag_guid from tag_profiles"))
        df_tag_prog = pd.DataFrame(result.fetchall())
        df_tag_prog.columns = result.keys()

    # drop oil refining and RMI cost center
    df_tag_prog = df_tag_prog[df_tag_prog['cost_center'] != 'RMI']
    df_tag_prog = df_tag_prog[df_tag_prog['cost_center'] != 'Oil Refining']

    # print unique cost centers
    print('Unique cost centers: ', df_tag_prog['cost_center'].unique())
    # Merge tag profiles to tag data, keeping all matches
    df_import_f2 = pd.merge(df_import_f2, df_tag_prog, how='outer', left_on='guid', right_on='tag_guid')

    # print number of tags that do not have a cost center
    print('Number of tags without a cost center: ', len(df_import_f2[df_import_f2['cost_center'].isnull()]))
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

    # print length of tag_profiles where cost_center is not null
    print('Number of content_ids with a cost center: ', len(tag_profiles))

    # drop oil refining and RMI cost center
    tag_profiles.drop(tag_profiles[tag_profiles['cost_center'] == 'RMI'].index, inplace = True)
    tag_profiles.drop(tag_profiles[tag_profiles['cost_center'] == 'Oil Refining'].index, inplace = True)


    # change content_id to integer
    tag_profiles['content_id'] = tag_profiles['content_id'].astype(int)

    # Write out backup and import to MySQL
    df_import_f2.to_excel(file)

    # drop tag_cat_y and rename tag_cat_x to tag_cat
    # df_import_f2 = df_import_f2.drop(['tag_cat_y'], axis=1)
    # df_import_f2.rename(columns={'tag_cat_x':'tag_cat'}, inplace=True)

    # subset to columns present in MySQL table
    df_import_f2 = df_import_f2[['content_id','tag_cat','tag','tag_guid', 'cost_center']]

    # set nan cost_center to null
    df_import_f2['cost_center'] = df_import_f2['cost_center'].replace('nan', '')

    # print number of rows to import
    print('Number of rows to import: ', len(df_import_f2))
    # import to MySQL
    df_import_f2.to_sql(con=database_connection, name='portal_content_tags', if_exists='append', index=False)

    # print number of rows to import
    print('Number of profiles to import: ', len(tag_profiles))

    # update cost_center in portal_live
    with database_connection.connect() as conn:
        for index, row in tag_profiles.iterrows():
            conn.execute(text("update portal_live set profiles = :cost_center where id = :content_id"), {'cost_center' : row['cost_center'], 'content_id' : row['content_id']})
            conn.commit()

    # Close connections
    database_connection.dispose()
    conn.close()

    # return success or failure message
    if len(df_import_f2) > 0:
        return 'Success'
    else:
        return 'Failure'