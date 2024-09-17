import re
import numpy as np
import pandas as pd
from datetime import timedelta
import requests
import json
import io
from io import StringIO
from datetime import datetime
import sys
import telebot
from google.oauth2.service_account import Credentials
import datetime as dt
from requests.exceptions import ConnectionError
from time import sleep
import gspread
from sqlalchemy import create_engine, text
import pymysql
import subprocess
import time

def run_script():
    try:
        now = datetime.now() + timedelta(hours=3)
        current_hour = now.hour

        username = ''
        password = ''
        host = ''
        port = ''
        database = ''
        table_name = ''

        engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}', pool_pre_ping=True)
        connection = engine.connect()

        try:

            sql_update = f'''
                    UPDATE {table_name} 
                    SET type = NULL, new_cpc = NULL, new_budget = NULL, date_1 = NOW(), date_2 = NULL 
                    WHERE date_2 IS NULL
                '''
            connection.execute(text(sql_update))
            time.sleep(5)
            # SQL query to select records
            sql_select = f'''
                    SELECT game_id, direct_id AS sql_direct_id, new_cpc AS sql_cpc, new_budget AS sql_budget, type as type_sql, ROI as sql_ROI, date_1, date_2 FROM {table_name}'''
            df_sql = pd.read_sql_query(text(sql_select), connection)
        finally:
            connection.close()


        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

        # Метрика API
        url = 'https://api-metrika.yandex.net/stat/v1/data.csv'
        with open('', 'r', encoding='utf-8') as file:
            counter_id = file.read()
        metrics = 'ym:s:visits'
        dimensions = 'ym:s:startURL, ym:s:counterID'

        params = {
            'ids': counter_id,
            'metrics': metrics,
            'dimensions': dimensions,
            'date1': datetime.now().strftime("%Y-%m-%d"),
            'date2': datetime.now().strftime("%Y-%m-%d"),
            'limit': 100000
        }

        headers = {
            "Authorization": "OAuth "
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            df_data = pd.read_csv(StringIO(response.text))
        else:
            print("error:", response.status_code)


        def extract_six_digit_number(url):
            match = re.search(r'\b\d{6}\b', url)
            return match.group(0) if match else None


        def extract_number(url):
            match = re.search(r'\d+', url)
            return match.group(0) if match else None


        df_data['Landing page'] = df_data['Landing page'].apply(extract_six_digit_number)
        df_data = df_data.groupby(['Tag', 'Landing page']).sum().reset_index()
        df_data['Tag'] = df_data['Tag'].apply(extract_number)


        ReportsURL = ''

        token = ''

        clientLogin = ''

        headers = {
            "Authorization": "Bearer " + token,
            "Client-Login": clientLogin,
            "Accept-Language": "en",
            "processingMode": "auto"
        }

        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                },
                "FieldNames": ["Name", "Id", "State"],
                "TextCampaignFieldNames": ["BiddingStrategy"]
            }
        }

        body = json.dumps(body, indent=4)

        req = requests.post(ReportsURL, body, headers=headers)


        report_content = req.text.split('\t')
        data = json.loads(report_content[0])

        campaigns = data['result']['Campaigns']

        df_state = pd.DataFrame(campaigns)


        def extract_weekly_spend_limit(row):
            try:
                return row['BiddingStrategy']['Search']['WbMaximumConversionRate']['WeeklySpendLimit']
            except KeyError:
                return None


        def extract_bid_ceiling(row):
            try:
                return row['BiddingStrategy']['Search']['WbMaximumConversionRate']['BidCeiling']
            except KeyError:
                return None


        df_state['WeeklySpendLimit'] = df_state['TextCampaign'].apply(extract_weekly_spend_limit).apply(
            lambda x: round(x / 1000000, 3))
        df_state['BidCeiling'] = df_state['TextCampaign'].apply(extract_bid_ceiling).apply(lambda x: round(x / 1000000, 3))
        df_state.drop(columns=['TextCampaign'], inplace=True)
        df_state.rename(columns={'Id': 'CampaignId'}, inplace=True)
        df_state = df_state[(df_state["State"] == 'ON')]


        ReportsURL = ''

        token = ''

        clientLogin = ''

        headers = {
            "Authorization": "Bearer " + token,
            "Client-Login": clientLogin,
            "Accept-Language": "en",
            "processingMode": "auto"
        }

        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": datetime.now().strftime("%Y-%m-%d"),
                    "DateTo": datetime.now().strftime("%Y-%m-%d")
                },
                "FieldNames": [
                    "CampaignId",
                    "CampaignName",
                    "Clicks",
                    "Cost",
                ],
                "ReportName": "",
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }

        body = json.dumps(body, indent=4)

        req = requests.post(ReportsURL, body, headers=headers)

        report_content = req.text.split('\t')
        df_dir = pd.read_csv(io.StringIO(req.text), sep='\t', header=1)
        df_dir["Cost"] = df_dir["Cost"].apply(lambda x: round(x / 1000000, 3))
        df_dir = df_dir.dropna(subset=['Cost'])
        df_dir["CampaignId"] = df_dir["CampaignId"].astype(int)
        df_state["CampaignId"] = df_state["CampaignId"].astype(int)
        df_dir = pd.merge(df_dir, df_state, on='CampaignId', how='inner')
        df_dir['Name'] = df_dir['Name'].str[6:]
        df = pd.merge(df_data, df_dir, left_on='Landing page', right_on='Name', how='inner')

        ReportsURL = ''

        token = ''

        clientLogin = ''

        headers = {
            "Authorization": "Bearer " + token,
            "Client-Login": clientLogin,
            "Accept-Language": "en",
            "processingMode": "auto"
        }

        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                    "DateTo": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                },
                "FieldNames": [
                    "CampaignId",
                    "Cost"
                ],
                "ReportName": "dgdFDweSDCfdd",
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }

        body = json.dumps(body, indent=4)

        req = requests.post(ReportsURL, body, headers=headers)


        report_content = req.text.split('\t')
        df_dir_2 = pd.read_csv(io.StringIO(req.text), sep='\t', header=1)
        df_dir_2["Cost_-1"] = df_dir_2["Cost"].apply(lambda x: round(x / 1000000, 3))
        df_dir_2 = df_dir_2.dropna(subset=['Cost'])
        df_dir_2 = df_dir_2[['CampaignId', 'Cost_-1']]
        df_dir_2["CampaignId"] = df_dir_2["CampaignId"].astype(int)
        df = pd.merge(df, df_dir_2, on='CampaignId', how='inner')

        url = ''
        headers = {
            'Authorization': 'OAuth '
        }
        params = {
            'lang': 'ru',
            'pretty': '1',
            'dimension_field': ['date'],
            'period': [datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")],
            'entity_field': ['page_caption', 'page_id'],
            'field': ['partner_wo_nds', 'cpmv_partner_wo_nds', 'shows', 'ctr_direct'],
            'limit': 100000
        }

        response = requests.get(url, headers=headers, params=params)
        json_data = response.json()

        def json_to_dataframe(json_data):
            data = json_data['data']['points']
            df = pd.DataFrame(data)
            df['page_caption'] = df['dimensions'].apply(lambda x: x['page_caption'])
            df['page_id'] = df['dimensions'].apply(lambda x: x['page_id'])
            df['partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['partner_wo_nds'])
            df['cpmv_partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['cpmv_partner_wo_nds'])
            df['shows'] = df['measures'].apply(lambda x: x[0]['shows'])
            df['ctr_direct'] = df['measures'].apply(lambda x: x[0]['ctr_direct'])
            df.drop(columns=['dimensions', 'measures'], inplace=True)
            return df

        df_rsa = json_to_dataframe(json_data)

        url = ''
        headers = {
            'Authorization': 'OAuth '
        }
        params = {
            'lang': 'ru',
            'pretty': '1',
            'dimension_field': ['date|day'],
            'period': [(datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
                       (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")],
            'entity_field': ['page_id'],
            'field': ['cpmv_partner_wo_nds', 'partner_wo_nds'],
            'limit': 100000
        }

        response = requests.get(url, headers=headers, params=params)
        json_data = response.json()

        def json_to_dataframe(json_data):
            data = json_data['data']['points']
            df = pd.DataFrame(data)
            df['date'] = df['dimensions'].apply(lambda x: x['date'][0])
            df['page_id'] = df['dimensions'].apply(lambda x: x['page_id'])
            df['cpmv_partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['cpmv_partner_wo_nds'])
            df['partner_wo_nds'] = df['measures'].apply(lambda x: x[0]['partner_wo_nds'])
            df.drop(columns=['dimensions', 'measures'], inplace=True)
            return df

        df_rsa_2 = json_to_dataframe(json_data)

        df_rsa_2['CPMV_-1'] = df_rsa_2.apply(
            lambda row: row['cpmv_partner_wo_nds'] if row['date'] == (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d") else None, axis=1)
        df_rsa_2['CPMV_-2'] = df_rsa_2.apply(
            lambda row: row['cpmv_partner_wo_nds'] if row['date'] == (datetime.now() - timedelta(days=2)).strftime(
                "%Y-%m-%d") else None, axis=1)
        df_rsa_2['Revenue_-1'] = df_rsa_2.apply(
            lambda row: row['partner_wo_nds'] if row['date'] == (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d") else None, axis=1)
        df_rsa_2 = df_rsa_2[['page_id', 'CPMV_-1', 'CPMV_-2', 'Revenue_-1']]


        df_rsa_2 = df_rsa_2.groupby(['page_id']).max()
        df_rsa = pd.merge(df_rsa, df_rsa_2, on='page_id', how='inner')

        df['Tag'] = df['Tag'].astype(int)
        df = pd.merge(df, df_rsa, left_on='Tag', right_on='page_id', how='inner')
        df.rename(columns={'page_caption': 'Game Name'}, inplace=True)
        df['Game Name'] = df['Game Name'].str[12:]
        df.rename(columns={'partner_wo_nds': 'Revenue', 'cpmv_partner_wo_nds': 'CPMV', 'Landing page': 'game_id',
                           'Game Name': 'game_name', 'CampaignId': 'direct_id', 'Tag': 'metrica_id',
                           'WeeklySpendLimit': 'old_budget', 'BidCeiling': 'old_cpc'}, inplace=True)
        df['shows_per_user'] = (df['shows'] / df['Sessions']).round(2)
        df['ARPU'] = df['Revenue'] / df['Sessions']
        df['ARPU'] = df['ARPU'].round(3)
        df["CPC"] = (df['Cost'] / df['Clicks']).round(3)
        df['ROI'] = ((df['Revenue'] - df['Cost'])) / df['Cost']
        df['ROI'] = df['ROI'].round(2)
        df['ROI_-1'] = ((df['Revenue_-1'] - df['Cost_-1'])) / df['Cost_-1']
        df['ROI_-1'] = df['ROI_-1'].round(2)
        df['Organic'] = ((1 - (df['Clicks'] / df['Sessions']))).round(2)
        df['Net Profit'] = (df['Revenue'] - df['Cost']).round(0)
        df['url'] = 'https://games.yandex.ru/console/application/' + df['Name'] + '#direct'

        df['CPMV_flag'] = np.where((df['CPMV'] < 500) & (df['CPMV_-1'] < 500) & (df['CPMV_-2'] < 500), 1, 0)
        df['CPMV_bust'] = np.where(
            (df['CPMV'] > df['CPMV_-1'] * 1.2) & (df['CPMV_-1'] > df['CPMV_-2'] * 1.2) & (df['CPMV'] > 1000), 1, 0)
        df['ROI_flag'] = np.where((df['ROI'] > 1) & (df['ROI_-1'] > 1), 1, 0)
        df = df[
            ['game_id', 'game_name', 'Organic', 'ROI_flag', 'direct_id', 'metrica_id', 'old_budget', 'old_cpc', 'CPMV',
             'CPMV_flag',
             'CPMV_bust',
             'Cost', 'Revenue',
             'ROI', 'Net Profit', 'ARPU']]
        df['game_id'] = df['game_id'].astype(int)
        df = pd.merge(df, df_sql, on='game_id', how='left')


        def first_non_nat(*values):
            for value in values:
                if pd.notna(value):
                    return value
            return None


        df_new = df[
            pd.isna(df['date_1']) &
            pd.isna(df['date_2']) &
            pd.isna(df['type_sql']) &
            pd.isna(df['sql_direct_id'])
            ]

        if not df_new.empty:
            df_new['date_1'] = now
            df = df.drop(df_new.index)

        for i, row_data in df.iterrows():

            if row_data['type_sql'] == "restart_up" or row_data['type_sql'] == "restart_push":

                if (row_data['type_sql'] == "restart_up" and pd.isna(row_data['date_2']) == False and row_data[
                    'CPMV_flag'] == 1 and
                        (datetime.now() - row_data['date_2']).components.days >= 3):
                    if row_data['ROI'] < 0:
                        df.loc[i, 'type'] = 'cpc'
                        df.loc[i, 'new_budget'] = row_data['old_budget']
                        df.loc[i, 'new_cpc'] = 0.3
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 1
                    else:
                        df.loc[i, 'type'] = None
                        df.loc[i, 'new_budget'] = None
                        df.loc[i, 'new_cpc'] = None
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 1

                elif 500 < row_data['CPMV'] < 1500 and (pd.isna(row_data['date_2']) == False or
                                                        (now - first_non_nat(row_data['date_2'],
                                                                             row_data['date_1'])).components.hours >= 2) and \
                        row_data['ROI'] > 0.3:
                    df.loc[i, 'type'] = 'restart_push'
                    if 0.3 < row_data['ROI'] <= 0.6:
                        df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.1
                    elif 0.3 < row_data['ROI'] <= 1:
                        df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.2
                    elif row_data['ROI'] > 1:
                        df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.3
                    df.loc[i, 'new_cpc'] = row_data['old_cpc']
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = None
                    df.loc[i, 'logic_flag'] = 2

                elif row_data['type_sql'] == 'restart_push' and row_data['ROI'] < 0.1 and row_data['old_cpc'] > row_data[
                    'ARPU'] + 0.1:
                    df.loc[i, 'type'] = 'cpc'
                    df.loc[i, 'new_budget'] = row_data['old_budget']
                    if row_data['ARPU'] < 0.3:
                        df.loc[i, 'new_cpc'] = 0.3
                    else:
                        df.loc[i, 'new_cpc'] = row_data['ARPU']
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = None
                    df.loc[i, 'logic_flag'] = 3

                elif row_data['type_sql'] == 'restart_push' and row_data['ROI'] < -0.1 and row_data['CPMV'] < 500 and row_data[
                    'Cost'] > 100:
                    df.loc[i, 'type'] = 'cpc'
                    df.loc[i, 'new_budget'] = row_data['old_budget']
                    df.loc[i, 'new_cpc'] = 0.3
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = None
                    df.loc[i, 'logic_flag'] = 4

                elif row_data['type_sql'] == 'restart_up':
                    df.loc[i, 'type'] = 'restart_up'
                    df.loc[i, 'new_budget'] = row_data['old_budget']
                    df.loc[i, 'new_cpc'] = 1
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = row_data['date_2']
                    df.loc[i, 'logic_flag'] = 5

            elif row_data['type_sql'] == "restart_up":
                df.loc[i, 'type'] = 'restart_up'
                df.loc[i, 'new_budget'] = row_data['old_budget']
                df.loc[i, 'new_cpc'] = row_data['old_cpc']
                df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                df.loc[i, 'date_2'] = None
                df.loc[i, 'logic_flag'] = 6

            elif (row_data['type_sql'] == 'push' or pd.isna(row_data['type_sql']) == True) \
                    and row_data['ROI'] < (row_data['sql_ROI'] - 0.1) and row_data['ROI'] < 0.1 and row_data[
                'old_budget'] > 300 and row_data['old_cpc'] > row_data['ARPU'] + 0.1:
                df.loc[i, 'type'] = 'cpc'
                df.loc[i, 'new_budget'] = row_data['old_budget']
                if row_data['ARPU'] < 0.3:
                    df.loc[i, 'new_cpc'] = 0.3
                else:
                    df.loc[i, 'new_cpc'] = row_data['ARPU']
                df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                df.loc[i, 'date_2'] = None
                df.loc[i, 'logic_flag'] = 7

            elif (row_data['type_sql'] != 'restart' and (row_data['type_sql']) != 'restart_up') and row_data[
                'old_budget'] > 10000 and row_data['ROI'] < 0 and row_data['old_cpc'] > row_data['ARPU'] + 0.1:
                df.loc[i, 'type'] = 'cpc'
                df.loc[i, 'new_budget'] = row_data['old_budget']
                if row_data['ARPU'] < 0.3:
                    df.loc[i, 'new_cpc'] = 0.3
                else:
                    df.loc[i, 'new_cpc'] = row_data['ARPU']
                df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                df.loc[i, 'date_2'] = None
                df.loc[i, 'logic_flag'] = 8

            elif (row_data['Cost'] > 100 and row_data['ROI'] * 100 < -20 and row_data['CPMV'] < 500 and row_data[
                'old_budget'] > 300 and row_data['type_sql'] != 'restart' and row_data['type_sql'] != 'restart_up'):
                df.loc[i, 'type'] = 'cpc'
                df.loc[i, 'new_budget'] = row_data['old_budget']
                df.loc[i, 'new_cpc'] = 0.3
                df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                df.loc[i, 'date_2'] = None
                df.loc[i, 'logic_flag'] = 9

            elif row_data['type_sql'] != 'restart' and row_data['type_sql'] != 'restart_up':
                if (first_non_nat(row_data['date_2'], row_data['date_1']) == row_data['date_2'] and (
                        now - row_data['date_2']).components.hours >= 4) or \
                        (first_non_nat(row_data['date_2'], row_data['date_1']) == row_data['date_1'] and (
                                now - row_data['date_1']).components.hours >= 1):

                    if (row_data['old_budget'] / 28) > row_data['Cost'] and row_data['old_cpc'] < 1.7 and row_data['ROI'] > 0:
                        if row_data['old_budget'] >= 10000 and row_data['ROI'] > 0.3:
                            df.loc[i, 'type'] = 'cpc'
                            df.loc[i, 'new_budget'] = row_data['old_budget']
                            if row_data['Organic'] > 0.3 and row_data['old_cpc'] < row_data['ARPU']:
                                if row_data['ARPU'] * 1.1 <= 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['ARPU'] * 1.1
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            else:
                                if row_data['old_cpc'] * 1.3 < 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['old_cpc'] * 1.3
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 10
                        elif row_data['old_budget'] < 10000 and row_data['ROI'] > 0:
                            df.loc[i, 'type'] = 'cpc'
                            df.loc[i, 'new_budget'] = row_data['old_budget']
                            if row_data['Organic'] > 0.3 and row_data['old_cpc'] < row_data['ARPU']:
                                if row_data['ARPU'] * 1.1 <= 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['ARPU'] * 1.1
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            else:
                                if row_data['old_cpc'] * 1.3 < 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['old_cpc'] * 1.3
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 10

                    elif (row_data['old_budget'] / 28) > row_data['Cost'] and row_data['old_cpc'] >= 1.7 and now.hour > 13 and row_data['ROI'] > 0:
                        if row_data['old_budget'] >= 10000 and row_data['ROI'] > 0.3:
                            df.loc[i, 'type'] = 'cpc'
                            df.loc[i, 'new_budget'] = row_data['old_budget']
                            if row_data['Organic'] > 0.3 and row_data['old_cpc'] < row_data['ARPU']:
                                if row_data['ARPU'] * 1.1 <= 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['ARPU'] * 1.1
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            else:
                                if row_data['old_cpc'] * 1.3 < 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['old_cpc'] * 1.3
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 10
                        elif row_data['old_budget'] < 10000 and row_data['ROI'] > 0:
                            df.loc[i, 'type'] = 'cpc'
                            df.loc[i, 'new_budget'] = row_data['old_budget']
                            if row_data['Organic'] > 0.3 and row_data['old_cpc'] < row_data['ARPU']:
                                if row_data['ARPU'] * 1.1 <= 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['ARPU'] * 1.1
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            else:
                                if row_data['old_cpc'] * 1.3 < 1.7:
                                    df.loc[i, 'new_cpc'] = row_data['old_cpc'] * 1.3
                                else:
                                    df.loc[i, 'new_cpc'] = 1.7
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 11

                    elif row_data['CPMV'] > 500 and row_data['ROI'] * 100 > 20 and row_data['old_budget'] < 10000:

                        if 1500 < row_data['old_budget'] < 6000:
                            df.loc[i, 'type'] = 'push'
                            df.loc[i, 'new_budget'] = row_data['old_budget'] + 1000
                            df.loc[i, 'new_cpc'] = row_data['old_cpc']
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 12

                        elif 300 < row_data['old_budget'] <= 1500 and row_data['ROI'] * 100 > 50:
                            df.loc[i, 'type'] = 'push'
                            df.loc[i, 'new_budget'] = 4000
                            df.loc[i, 'new_cpc'] = row_data['old_cpc']
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 13

                        elif 6000 <= row_data['old_budget'] < 10000 and row_data['ROI'] * 100 > 60:
                            df.loc[i, 'type'] = 'push'
                            df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.2
                            df.loc[i, 'new_cpc'] = row_data['old_cpc']
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 14

                        elif row_data['old_budget'] == 300 and row_data['ROI'] * 100 > 40:
                            df.loc[i, 'type'] = 'push'
                            df.loc[i, 'new_budget'] = 1500
                            df.loc[i, 'new_cpc'] = row_data['old_cpc']
                            df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                            df.loc[i, 'date_2'] = None
                            df.loc[i, 'logic_flag'] = 15

                    elif row_data['CPMV_bust'] == 1 and row_data['old_budget'] < 15000 and row_data['ROI'] > 0:
                        df.loc[i, 'type'] = 'push'
                        df.loc[i, 'new_budget'] = row_data['old_budget'] + 1000
                        df.loc[i, 'new_cpc'] = row_data['old_cpc']
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 16

                    elif row_data['ROI'] * 100 < 10 and row_data['old_budget'] < 10000 and row_data['old_cpc'] > row_data[
                        'ARPU'] + 0.1:
                        df.loc[i, 'type'] = 'cpc'
                        df.loc[i, 'new_budget'] = row_data['old_budget']
                        if row_data['ARPU'] < 0.3:
                            df.loc[i, 'new_cpc'] = 0.3
                        else:
                            df.loc[i, 'new_cpc'] = row_data['ARPU']
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 17

                    elif row_data['ROI'] * 100 < 40 and row_data['old_budget'] >= 10000 and row_data['old_cpc'] > row_data[
                        'ARPU'] + 0.1:
                        df.loc[i, 'type'] = 'cpc'
                        df.loc[i, 'new_budget'] = row_data['old_budget']
                        if row_data['ARPU'] < 0.3:
                            df.loc[i, 'new_cpc'] = 0.3
                        else:
                            df.loc[i, 'new_cpc'] = row_data['ARPU']
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 18

                    elif row_data['ROI'] * 100 > 100 and 10000 <= row_data['old_budget'] < 30000:
                        df.loc[i, 'type'] = 'push'
                        if row_data['old_budget'] * 1.2 <= 30000:
                            df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.2
                        else:
                            df.loc[i, 'new_budget'] = 30000
                        df.loc[i, 'new_cpc'] = row_data['old_cpc']
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 19

                    elif row_data['ROI_flag'] == 1 and 30000 <= row_data['old_budget'] < 40000:
                        df.loc[i, 'type'] = 'push'
                        if row_data['old_budget'] * 1.1 <= 40000:
                            df.loc[i, 'new_budget'] = row_data['old_budget'] * 1.1
                        else:
                            df.loc[i, 'new_budget'] = 40000
                        df.loc[i, 'new_cpc'] = row_data['old_cpc']
                        df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                        df.loc[i, 'date_2'] = None
                        df.loc[i, 'logic_flag'] = 20

            if row_data['CPMV'] < 500 and row_data['ROI'] * 100 < 0 and row_data['old_budget'] == 300:
                df.loc[i, 'type'] = 'restart'
                df.loc[i, 'new_budget'] = 1500
                df.loc[i, 'new_cpc'] = None
                df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                df.loc[i, 'date_2'] = None
                df.loc[i, 'logic_flag'] = 21

            if row_data['type_sql'] == 'restart':
                if row_data['direct_id'] != row_data['sql_direct_id'] and pd.isna(row_data['date_2']) == False:
                    df.loc[i, 'type'] = 'restart_up'
                    df.loc[i, 'new_budget'] = None
                    df.loc[i, 'new_cpc'] = 1
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = None
                    df.loc[i, 'logic_flag'] = 22
                elif pd.isna(row_data['date_2']) == True:
                    df.loc[i, 'type'] = 'restart'
                    df.loc[i, 'new_budget'] = row_data['sql_budget']
                    df.loc[i, 'new_cpc'] = row_data['sql_cpc']
                    df.loc[i, 'date_1'] = now.strftime("%Y-%m-%d-%H-%M-%S")
                    df.loc[i, 'date_2'] = row_data['date_2']
                    df.loc[i, 'logic_flag'] = 23

        df['sql_cpc'] = df['sql_cpc'].astype(float)
        df['sql_budget'] = df['sql_budget'].astype(float)
        df = df.replace({None: np.nan})

        df['new_cpc'] = df['new_cpc'].apply(lambda x: round(float(x), 2) if pd.notnull(x) else x)
        df['new_budget'] = df['new_budget'].apply(lambda x: round(float(x), 0) if pd.notnull(x) else x)

        def equals_with_nan(a, b):
            return (a == b) | (pd.isna(a) & pd.isna(b))

        type_condition = equals_with_nan(df['type'], df['type_sql'])
        budget_condition = equals_with_nan(df['new_budget'], df['sql_budget'])
        cpc_condition = equals_with_nan(df['new_cpc'], df['sql_cpc'])
        combined_condition = type_condition & budget_condition & cpc_condition

        df = df[~combined_condition]
        df = df.drop(columns=['type_sql', 'sql_cpc', 'sql_budget', 'sql_direct_id', 'sql_ROI', 'Organic', 'ROI_flag'])
        df = df[(df['Cost'] > 0)]
        df = df.dropna(subset=['type', 'new_budget', 'new_cpc'], how='all')
        df['logic_flag'] = df['logic_flag'].astype(int)

        main_table_name = ''
        temp_table_name = ''

        connection = pymysql.connect(host='', port=, user='', password='', db='')
        engine = create_engine('mysql+pymysql://', creator=lambda: connection)

        if not df_new.empty:
            df_new = df_new.drop(columns=['type_sql', 'sql_cpc', 'sql_budget', 'sql_direct_id', 'sql_ROI'])
            df_new.to_sql(main_table_name, engine, if_exists='append', index=False)

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                CREATE TEMPORARY TABLE {temp_table_name} LIKE {main_table_name};
                """)
                connection.commit()

            df.to_sql(temp_table_name, engine, if_exists='append', index=False)

            with connection.cursor() as cursor:
                cursor.execute(f"""
                UPDATE {main_table_name} main
                JOIN {temp_table_name} temp ON main.game_id = temp.game_id
                SET 
                    main.type = temp.type,
                    main.old_budget = temp.old_budget,
                    main.new_budget = temp.new_budget,
                    main.old_cpc = temp.old_cpc,
                    main.new_cpc = temp.new_cpc,
                    main.CPMV = temp.CPMV,
                    main.CPMV_flag = temp.CPMV_flag,
                    main.CPMV_bust = temp.CPMV_bust,
                    main.Revenue = temp.Revenue,
                    main.Cost = temp.Cost,
                    main.ROI = temp.ROI,
                    main.ARPU = temp.ARPU,
                    main.`Net Profit` = temp.`Net Profit`,
                    main.date_1 = temp.date_1,
                    main.date_2 = temp.date_2,
                    main.direct_id = temp.direct_id,
                    main.logic_flag = temp.logic_flag
                """)
                connection.commit()

            print("Data successfully updated")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            connection.close()

        df_cpc = df[(df['type'] == '''cpc''')]
        df_push = df[(df['type'] == '''push''')]
        df_restart = df[df['type'].str.startswith('restart')]
        df_stop = df[(df['type'] == '''stop''')]
        output_text = f"🚨 *New Example Signals* 🚨\n"
        if not df_cpc.empty:
            output_text += f"\n💲 *CPC* 💲\n"
            for i, row in enumerate(df_cpc.iterrows()):
                if row[1]['old_cpc'] < row[1]['new_cpc']:
                    output_text += f"{i + 1}. *{row[1]['game_name']}*: *ROI* {row[1]['ROI'] * 100:.0f}% \|\| Raise CPC from {row[1]['old_cpc']} to {row[1]['new_cpc']}.\n"
                else:
                    output_text += f"{i + 1}. *{row[1]['game_name']}*: *ROI* {row[1]['ROI'] * 100:.0f}% \|\| Lower CPC from {row[1]['old_cpc']} to {row[1]['new_cpc']}.\n"

        if not df_push.empty:
            output_text += f"\n🚀 *PUSH* 🚀\n"
            for i, row in enumerate(df_push.iterrows()):
                output_text += f"{i + 1}. *{row[1]['game_name']}*: *ROI* {row[1]['ROI'] * 100:.0f}% \|\| Raise budget from {row[1]['old_budget']} to {row[1]['new_budget']}.\n"

        if not df_restart.empty:
            output_text += f"\n🔄 *RESTART* 🔄\n"
            for i, row in enumerate(df_restart.iterrows()):
                if row[1]['type'] == 'restart':
                    output_text += f"{i + 1}. *{row[1]['game_name']}*: New campaign restart. *Action Required*: Set up the new campaign.\n"
                elif row[1]['type'] == 'restart_push':
                    output_text += f"{i + 1}. *{row[1]['game_name']}*: *ROI* {row[1]['ROI'] * 100:.0f}% \|\| Raise budget of the restarted campaign from {row[1]['old_budget']} to {row[1]['new_budget']}.\n"
                elif row[1]['type'] == 'restart_up':
                    output_text += f"{i + 1}. *{row[1]['game_name']}*: Campaign restarted, lower CPC from {row[1]['old_cpc']} to {row[1]['new_cpc']}.\n"

        if not df_stop.empty:
            output_text += f"\n⛔ *STOP* ⛔\n"
            for i, row in enumerate(df_stop.iterrows()):
                output_text += f"{i + 1}. *{row[1]['game_name']}*: *ROI* {row[1]['ROI'] * 100:.0f}% \|\| Stop the campaign.\n"

        bot_token = ''
        # Replace 'YOUR_CHAT_ID' with your chat ID
        chat_id = ''
        # Replace 'YOUR_MESSAGE_TEXT' with your message
        message_text = output_text
        message_text = message_text.replace('.', r'\.').replace('-', r'\-').replace('=', r'\=').replace('!', r'\!')

        bot = telebot.TeleBot(bot_token)

        if not message_text == "🚨 *Example* 🚨\n":
            bot.send_message(chat_id, message_text, parse_mode='MarkdownV2')

        print("Example")
        return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False


max_retries = 5
attempt = 0

while attempt < max_retries:
    if run_script():
        break
    else:
        attempt += 1
        if attempt < max_retries:
            sleep(1)
        else:
            print("Attempt limit reached. The script was not executed")
