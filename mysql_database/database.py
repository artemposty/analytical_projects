import re
import pandas as pd
from datetime import timedelta, datetime
import requests
import json
from sqlalchemy import create_engine
import pymysql

# Current time setup
now = datetime.now() + timedelta(hours=3)
current_date = datetime.now().strftime("%Y-%m-%d")
current_time = now.strftime("%Y-%m-%d %H:%M")

# API keys placeholders (to be set)
direkt_key = ''
metrika_key = ''
rsa_key = ''

# Load account list from a file (file path needs to be added)
with open('', 'r', encoding='utf-8') as file:
    account_list = [login.strip() for login in file.read().strip('[]').split(',')]


# Function to extract a 6-digit number from a URL
def extract_six_digit_number(url):
    match = re.search(r'\b\d{6}\b', url)
    return match.group(0) if match else None


# Function to extract any number from a URL
def extract_number(url):
    match = re.search(r'\d+', url)
    return match.group(0) if match else None


# Convert JSON data into a DataFrame and process the relevant columns
def json_to_dataframe(json_data):
    data = json_data['data']['points']
    df = pd.DataFrame(data)

    # Extracting nested fields
    df['page_caption'] = df['dimensions'].apply(lambda x: x['page_caption'])
    df['page_id'] = df['dimensions'].apply(lambda x: x['page_id'])
    df['partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['partner_wo_nds'])
    df['cpmv_partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['cpmv_partner_wo_nds'])
    df['shows'] = df['measures'].apply(lambda x: x[0]['shows'])
    df['ctr_direct'] = df['measures'].apply(lambda x: x[0]['ctr_direct'])

    # Dropping unneeded columns
    df.drop(columns=['dimensions', 'measures'], inplace=True)

    return df


# Function to extract weekly spend limit from JSON, handling missing keys
def extract_weekly_spend_limit(row):
    try:
        return row['BiddingStrategy']['Search']['WbMaximumConversionRate']['WeeklySpendLimit']
    except KeyError:
        return None


# Function to extract bid ceiling from JSON, handling missing keys
def extract_bid_ceiling(row):
    try:
        return row['BiddingStrategy']['Search']['WbMaximumConversionRate']['BidCeiling']
    except KeyError:
        return None


# Function to process and upload DataFrame to the database
def upload_df(client_login):
    # Setup for Metрика API (url and client keys need to be set)
    url = ''

    # Load tags specific to the client (file path needs to be set)
    with open(f'{client_login}_tags.txt', 'r', encoding='utf-8') as tag_file:
        tags = tag_file.read().splitlines()

    # Placeholder for a DataFrame (to be fetched from API or another source)
    df = pd.DataFrame()  # Example: df = json_to_dataframe(fetch_data_from_api())

    # Data transformation steps
    df.rename(columns={'page_caption': 'Game Name'}, inplace=True)
    df['Game Name'] = df['Game Name'].str[12:]  # Clean up 'Game Name' field
    df.rename(columns={'partner_wo_nds': 'Revenue', 'cpmv_partner_wo_nds': 'CPMV'}, inplace=True)

    # Calculated fields
    df['shows_per_user'] = (df['shows'] / df['Sessions']).round(2)
    df['ARPU'] = (df['Revenue'] / df['Sessions']).round(2)
    df['CPC'] = (df['Cost'] / df['Clicks']).round(2)
    df['ROI'] = ((df['Revenue'] - df['Cost']) / df['Cost']).round(2)
    df['Organic'] = (1 - (df['Clicks'] / df['Sessions'])).round(2)
    df['Net Profit'] = (df['Revenue'] - df['Cost']).round(0)
    df['url'] = 'https://games.yandex.ru/console/application/' + df['Name'] + '#direct'
    df['time'] = current_time

    # Select relevant columns for upload
    df = df[['time', 'Game Name', 'Sessions', 'Clicks', 'shows', 'ctr_direct', 'Cost', 'Revenue', 'WeeklySpendLimit',
             'BidCeiling', 'total_avg_time', 'day_avg_time', 'bounce_sessions', 'single_sessions', 'mobile_sessions',
             'ad_block_sess', 'Name', 'CampaignId', 'Tag']]

    # Rename columns to database format
    df.rename(columns={
        'Game Name': 'game_name',
        'CampaignId': 'direct_id',
        'Sessions': 'sessions',
        'ctr_direct': 'ctr',
        'Cost': 'spend',
        'Revenue': 'revenue',
        'WeeklySpendLimit': 'budget',
        'BidCeiling': 'direct_cpc',
        'Clicks': 'direct_clicks',
        'shows': 'ad_shows',
        'Tag': 'metrika_id',
        'Name': 'game_id'
    }, inplace=True)

    # Database connection details (placeholders to be filled)
    username = ''
    password = ''
    host = ''
    port = ''
    database = ''
    table_name = f'{client_login}_data'

    # SQLAlchemy connection string
    connection_string = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    connection = create_engine(connection_string)

    # Upload DataFrame to SQL table
    df.to_sql(table_name, con=connection, if_exists='append', index=False)


# Process and upload data for each account in the list
for login in account_list:
    try:
        upload_df(login)
    except Exception as e:
        print(f"ERROR UPLOADING TABLE {login}: {e}")
