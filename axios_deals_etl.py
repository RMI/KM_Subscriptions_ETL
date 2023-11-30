
import pandas as pd
from pathlib import Path
import sqlalchemy
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import date, timezone, datetime
import shutil
import numpy as np
import regex
import re

load_dotenv('cred.env')
api_key = os.getenv('EXCHANGE_API')

url =  'https://v6.exchangerate-api.com/v6/' + str(api_key) + '/latest/USD'

print(url)

raw = pd.read_csv('axios_deals/data_raw/AxiosPro_ClimateDealsTracker_2023_07_17_full.csv')

regex.findall(r'\p{Sc}', raw['Deal Size'])

raw['Deal Size'].fillna('None', inplace=True)
raw.loc[raw['Deal Size'].astype(str).str.contains(r'\$', regex=True), 'Currency_sym'] = 'USD'
raw.loc[raw['Deal Size'].astype(str).str.contains(r'€', regex=True), 'Currency_sym'] = 'EUR'
raw.loc[raw['Deal Size'].astype(str).str.contains(r'CDN\$', regex=True), 'Currency_sym'] = 'CDN'
raw.loc[raw['Deal Size'].astype(str).str.contains(r'AUS\$', regex=True), 'Currency_sym'] = 'AUS'
raw.loc[raw['Deal Size'].astype(str).str.contains(r'\£', regex=True), 'Currency_sym'] = 'GBP'

raw.loc[raw['Deal Size'].astype(str).str.contains('None'), 'Currency_sym']= 'None'

raw['Deal Trim'] = raw['Deal Size']

raw['Deal Trim'] = raw['Deal Trim'].str.replace('None', '0').str.replace('AUS', "").str.replace(r'\$', "", regex=True).str.replace(
    'US', "").str.replace('CDN', "").str.replace(r'\€', "", regex=True).str.replace(r'\£', "", regex=True).str.strip()


# Create the conversion function
def converter(x):
    if '0' in x:
        return ""
    elif 'million' in x:
        return f"{(float(x.strip(' million'))*1000000):,.2f}"
    elif 'billion' in x:
        return f"{(float(x.strip(' billion'))*1000000000):,.2f}"

# Create numeric value
raw['value'] = raw['Deal Trim'].apply(converter)


# Convert to USD
# Need to find convertor package





        


raw.to_excel('axios_testing.xlsx')