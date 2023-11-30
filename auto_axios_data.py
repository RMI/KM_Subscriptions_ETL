import pandas as pd
from pathlib import Path
import sqlalchemy
from dotenv import load_dotenv
import os
from datetime import date
import numpy as np
import shutil
from sqlalchemy import text

mydir = Path("c:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/axios_deals")

load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')
# Define File Names
import_backup = 'c:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/axios_deals/archive/backup_'+ str(date.today()) + '.xlsx'

# Where you want the export to go after it's been processed
destinationpath = 'c:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/axios_deals/archive/'
sourcepath = "c:/Users/ghoffman/OneDrive - RMI/01. Projects/Python_General/axios_deals"

# database credentials
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_km_news'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
dfs = []

# Loop through data folder, appending any csv files to df
for file in mydir.glob('AxiosPro*.csv'):
    data = pd.read_csv(file)
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)

# Move new csv to archive
sourcefiles = os.listdir(sourcepath)
for file in sourcefiles:
    if file.startswith('AxiosPro'):
        shutil.move(os.path.join(sourcepath,file), os.path.join(destinationpath,file))


df.rename(columns={'Publish Date':'pubDate', 'Company Name':'company', 'Investors':'investors', 'Deal Size':'deal_size', 'Type':'type',
                   'Series/Round':'series_round'}, inplace=True)

df['pubDate'] = pd.to_datetime(df['pubDate'])


df['deal_size'].fillna('None', inplace=True)
df.loc[df['deal_size'].astype(str).str.contains(r'\$', regex=True), 'deal_currency'] = 'USD'
#df.loc[df['deal_size'].astype(str).str.contains(r'€', regex=True), 'deal_currency'] = 'EUR'
df.loc[df['deal_size'].astype(str).str.contains(r'\u20ac', regex=True), 'deal_currency'] = 'EUR'
df.loc[df['deal_size'].astype(str).str.contains(r'CDN\$', regex=True), 'deal_currency'] = 'CDN'
df.loc[df['deal_size'].astype(str).str.contains(r'AUS\$', regex=True), 'deal_currency'] = 'AUS'
#df.loc[df['deal_size'].astype(str).str.contains(r'\\£', regex=True), 'deal_currency'] = 'GBP'
df.loc[df['deal_size'].astype(str).str.contains(r'\xA3', regex=True), 'deal_currency'] = 'GBP'
df.loc[df['deal_size'].astype(str).str.contains('None'), 'deal_currency']= 'None'


df['deal_trim'] = df['deal_size']

df['deal_trim'] = df['deal_trim'].str.replace('None', '0').str.replace('AUS', "").str.replace(r'\$', "", regex=True).str.replace(
    'US', "").str.replace('CDN', "").str.replace(r'\u20ac', "", regex=True).str.replace(r'\xA3', "", regex=True).str.strip()

# Create the conversion function
def converter(x):
    if '0' in x:
        return ""
    elif 'million' in x:
        return f"{(float(x.strip(' million'))*1000000):,.2f}"
    elif 'billion' in x:
        return f"{(float(x.strip(' billion'))*1000000000):,.2f}"

# Create numeric value
df['deal_value'] = df['deal_trim'].apply(converter)

df['deal_value'] = df['deal_value'].str.replace(',','')
df['deal_value'] = df['deal_value'].replace('', '0.00')


with database_connection.connect() as conn:
    result = conn.execute(text("select pubDate, company, deal_size from axios_dashboard"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1['pubDate'] = df1['pubDate'].astype(str)
df1['deal_size'] = df1['deal_size'].astype(str)
df1['uid'] = df1['company'].astype(str)
df1['uid'] = df1['uid'].str.cat(df1['pubDate'], sep= "_")
df1['uid'] = df1['uid'].str.cat(df1['deal_size'], sep= "_")

df['uid'] = df['company'].astype(str)
df['uid'] = df['uid'].str.cat(df['pubDate'].astype(str), sep= "_")
df['uid'] = df['uid'].str.cat(df['deal_size'].astype(str), sep= "_")


df.set_index('uid')
df = df.drop(df[df.uid.isin(df1['uid'])].index.tolist())
df.reset_index(inplace=True)
df_import = df[['pubDate', 'company', 'investors', 'deal_size', 'type', 'series_round',
       'deal_currency', 'deal_value']]

# Export Excel backup
df_import.to_excel(import_backup)


# Import new records
df_import.to_sql(con=database_connection, name='axios_dashboard', if_exists='append', index=False)
