from datetime import timedelta
import datetime
import requests
import pandas as pd
import io
import re
import json
from io import StringIO
import telebot
import time

current_time = time.time() + (3 * 3600)
local_time = time.localtime(current_time)
hour = local_time.tm_hour
output_text = f"*{hour}:00* \n\n"
balance_text = '*Balance*: \n'
all_rev = 0
all_spend = 0
bot_token = ''
chat_id = ''
direkt_key = ''
metrika_key = ''
rsa_key = ''


with open('', 'r') as file:
    old_bot_json = json.load(file)

with open('account_logins.txt', 'r', encoding='utf-8') as file:
    account_list = [login.strip() for login in file.read().strip('[]').split(',')]

bot_json = {login: [] for login in account_list}

def extract_six_digit_number(url):
    match = re.search(r'\b\d{6}\b', url)
    return match.group(0) if match else None

def extract_number(url):
    match = re.search(r'\d+', url)
    return match.group(0) if match else None

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

def get_df(client_login):
    url = ''
    with open(f'{client_login}_tags.txt', 'r', encoding='utf-8') as file:
        counter_id = file.read()

    metrics = 'ym:s:visits'
    dimensions = 'ym:s:startURL, ym:s:counterID'

    params = {
        'ids': counter_id,
        'metrics': metrics,
        'dimensions': dimensions,
        'date1': datetime.datetime.now().strftime("%Y-%m-%d"),
        'date2': datetime.datetime.now().strftime("%Y-%m-%d"),
        'limit': 100000
    }

    headers = {
        "Authorization": "OAuth " + metrika_key
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        df_data = pd.read_csv(StringIO(response.text))
    else:
        print("Ошибка при выполнении запроса:", response.status_code)
        return

    df_data['Landing page'] = df_data['Landing page'].apply(extract_six_digit_number)
    df_data = df_data.groupby(['Tag', 'Landing page']).sum().reset_index()
    df_data['Tag'] = df_data['Tag'].apply(extract_number)

    ReportsURL = ''

    headers = {
        "Authorization": "Bearer " + direkt_key,
        "Client-Login": client_login,
        "Accept-Language": "ru",
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

    df_state['WeeklySpendLimit'] = df_state['TextCampaign'].apply(extract_weekly_spend_limit).apply(
        lambda x: round(x / 1000000, 3))
    df_state['BidCeiling'] = df_state['TextCampaign'].apply(extract_bid_ceiling).apply(lambda x: round(x / 1000000, 3))
    df_state.drop(columns=['TextCampaign'], inplace=True)
    df_state.rename(columns={'Id': 'CampaignId'}, inplace=True)
    df_state = df_state[(df_state["State"] == 'ON')]

    ReportsURL = ''

    headers = {
        "Authorization": "Bearer " + direkt_key,
        "Client-Login": client_login,
        "Accept-Language": "ru",
        "processingMode": "auto"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": datetime.datetime.now().strftime("%Y-%m-%d"),
                "DateTo": datetime.datetime.now().strftime("%Y-%m-%d")
            },
            "FieldNames": [
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Cost",
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
    df_dir = pd.read_csv(io.StringIO(req.text), sep='\t', header=1)
    df_dir["Cost"] = df_dir["Cost"].apply(lambda x: round(x / 1000000, 3))
    df_dir = df_dir.dropna(subset=['Cost'])
    df_dir["CampaignId"] = df_dir["CampaignId"].astype(int)
    df_state["CampaignId"] = df_state["CampaignId"].astype(int)
    df_dir = pd.merge(df_dir, df_state, on='CampaignId', how='inner')
    df_dir['Name'] = df_dir['Name'].str[6:]
    df_dir['Cost'] = (df_dir['Cost'] * 1.2).round(2)
    df = pd.merge(df_data, df_dir, left_on='Landing page', right_on='Name', how='inner')
    rk_count = df_dir.shape[0]

url = ''
headers = {
    'Authorization': 'OAuth ' + rsa_key
}
params = {
    'lang': 'ru',
    'pretty': '1',
    'dimension_field': ['date'],
    'period': [datetime.datetime.now().strftime("%Y-%m-%d"), datetime.datetime.now().strftime("%Y-%m-%d")],
    'entity_field': ['page_caption', 'page_id'],
    'field': ['partner_wo_nds', 'cpmv_partner_wo_nds', 'shows', 'ctr_direct'],
    'limit': 100000
}

response = requests.get(url, headers=headers, params=params)
json_data = response.json()

df_rsa = json_to_dataframe(json_data)
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
    df['Organic'] = ((1 - (df['Clicks'] / df['Sessions']))).round(2)
    df['Net Profit'] = (df['Revenue'] - df['Cost']).round(0)
    df['url'] = 'https://games.yandex.ru/console/application/' + df['Name'] + '#direct'
    cpmv = (df['Revenue'].sum() / df['shows'].sum()) * 1000
    df = df[
        ['game_id', 'game_name', 'direct_id', 'metrica_id', 'old_budget', 'old_cpc', 'CPMV',
         'Cost', 'Revenue',
         'ROI', 'Net Profit', 'ARPU']]
    df['game_id'] = df['game_id'].astype(int)

    # Balance
    url = 'https://api.direct.yandex.ru/live/v4/json/'

    token = direkt_key

    data = {
        "method": "AccountManagement",
        "token": token,
        "param": {
            "Action": "Get",
            "SelectionCriteria": {
                "Logins": [f"{client_login}"]
            }
        }
    }

    jdata = json.dumps(data, ensure_ascii=False).encode('utf8')

    headers = {}

    response = requests.post(url, data=jdata, headers=headers)

    data = json.loads(response.text)

    balance = float(data['data']['Accounts'][0]['Amount'])
    balance = round(balance)

    if not df.empty:
        spend = df['Cost'].sum().round(2)
        rev = df['Revenue'].sum().round(2)

        new_entry = {
            "spend": spend,
            "rev": rev
        }
        bot_json[f'{client_login}'].append(new_entry)

    old_data = old_bot_json.get(f'{client_login}', [{'rev': 0, 'spend': 0}])[0]
    new_data = bot_json.get(f'{client_login}', [{'rev': 0, 'spend': 0}])[0]

    rev_change = new_data['rev'] - old_data['rev']
    spend_change = new_data['spend'] - old_data['spend']

    text = f"📊 *{client_login}* \({rk_count} РК\) 📊\n\n"
    text += f"*Per hour*: Rev \+{(rev_change).round(0)}$ \|\| Spend: \+{(spend_change).round(0)}$\n"
    text += f"*Per day*: Rev {rev.round(0)}$ \|\| Spend: {spend.round(0)}$ \|\| ROI: {(((rev - spend) / spend) * 100).round(0)}% \|\| NP: {(rev - spend).round(0)}$ \|\| CPMV: {cpmv.round(0)}$\n\n"
    balance_text_fun = ''
    if balance < 7000:
        balance_text_fun += f"❗❗❗"
    balance_text_fun += f"{client_login}: {balance}р."
    if balance < 7000:
        balance_text_fun += f"❗❗❗"
    balance_text_fun += '\n'
    return text, balance_text_fun, rev, spend

for login in account_list:
    try:
        text, balance_text_fun, rev, spend = get_df(login)
        output_text += text
        balance_text += balance_text_fun
        all_rev += rev
        all_spend += spend
    except Exception as e:
        print(f"ERROR GETTING {login} DATA: {e}")
        output_text += f"ERROR GETTING {login} DATA: {e}"

result_text = f"📝*Summary*: Rev {(all_rev).round(0)}$ \|\| Spend: {(all_spend).round(0)}$ \|\| ROI: {((((all_rev) - (all_spend)) / (all_spend)) * 100).round(0)}% \|\| NP: {((all_rev) - (all_spend)).round(0)}$\n\n"
output_text += result_text
output_text += balance_text

with open('bot_json.json', 'w') as file:
    json.dump(bot_json, file, indent=4)

message_text = output_text
message_text = message_text.replace('.', r'\.').replace('-', r'\-').replace('=', r'\=').replace('!', r'\!')

bot = telebot.TeleBot(bot_token)

# Отправка запроса
bot.send_message(chat_id, message_text, parse_mode='MarkdownV2')
