import matplotlib.pyplot as plt
from io import BytesIO
import re
import pandas as pd
from datetime import timedelta
from datetime import datetime
import telebot
from telebot.types import InputMediaPhoto
import numpy as np
from sqlalchemy import create_engine, text as sql_text
import pymysql

current_date = datetime.now().strftime("%Y-%m-%d")

# Telegram Bot Token
bot_token = ''

# Telegram Chat ID
chat_id = ''

with open('', 'r', encoding='utf-8') as file: # Path to TXT file with account logins
    account_list = [login.strip() for login in file.read().strip('[]').split(',')]


def time_to_minutes(time):
    return time.hour * 60 + time.minute

# Time Boundaries
start_time = time_to_minutes(pd.to_datetime('', format='%H:%M').time())
end_time = time_to_minutes(pd.to_datetime('', format='%H:%M').time())

# Sorting Key
def sorting_key(time):
    minutes = time_to_minutes(time)
    if start_time <= minutes <= 1439:
        return minutes
    else:
        return minutes + 1440


def send_roiline(client_login):
    # MySQL DataBase Connection
    username = ''
    password = ''
    host = ''
    port = ''
    database = ''
    table_name = f'{client_login}_data' # Table name should contain client_login
    connection_string = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    connection = create_engine(connection_string)

    query = f'''SELECT HOUR(time) AS hour, MINUTE(time) as minute, ROUND(SUM(revenue), 0) AS revenue, ROUND(SUM(spend), 0) as spend FROM `{table_name}` WHERE time >= CURDATE() + INTERVAL 8 HOUR GROUP BY 1, 2 ORDER BY 1, 2'''

    try:
        df_sql = pd.read_sql_query(con=connection.connect(), sql=sql_text(query))
    except Exception as e:
        print(f"ERROR: {e}")

    df_sql['time'] = df_sql['hour'].astype(str).str.zfill(2) + ':' + df_sql['minute'].astype(str).str.zfill(2)
    df_sql = df_sql[['time', 'revenue', 'spend']]

    # ROI
    df_sql['roi'] = (df_sql['revenue'] - df_sql['spend']) / df_sql['spend']
    df_sql['time'] = pd.to_datetime(df_sql['time'], format='%H:%M').dt.time

    # Sorting
    df_sql['sort_key'] = df_sql['time'].apply(sorting_key)
    df_sql = df_sql.sort_values(by='sort_key').drop(columns='sort_key')

    # Time
    df_sql['time'] = df_sql['time'].apply(lambda x: x.strftime('%H:%M'))

    # Creating conditions with NumPy arrays
    condition_spend_greater = np.array(df_sql['spend'] > df_sql['revenue'])
    condition_revenue_greater = np.array(df_sql['revenue'] > df_sql['spend'])

    # ROI Line
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(df_sql['time'], df_sql['revenue'], label='Revenue', color='green')
    ax1.plot(df_sql['time'], df_sql['spend'], label='Spend', color='red')

    # Filling
    ax1.fill_between(df_sql['time'], df_sql['spend'], df_sql['revenue'],
                     where=condition_spend_greater,
                     color='lightcoral', alpha=0.5, label='ROI < 0')

    ax1.fill_between(df_sql['time'], df_sql['revenue'], df_sql['spend'],
                     where=condition_revenue_greater,
                     color='lightgreen', alpha=0.5, label='ROI > 0')

    ax1.set_ylabel('Value')
    ax1.set_title(f'ROI {client_login} {pd.Timestamp.now().date()}')
    ax1.grid(False)
    ax1.tick_params(axis='x', rotation=45)
    ax2 = ax1.twinx()
    ax2.plot(df_sql['time'], df_sql['roi'], label='ROI', color='blue', linestyle='--')
    ax2.set_ylabel('ROI')

    # Legend
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3)

    # Saving
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

# Connection to Telegram Bot
bot = telebot.TeleBot(bot_token)

# Sending message
roilines = [send_roiline(login) for login in account_list]
media_group = [InputMediaPhoto(media=buf) for buf in roilines]
bot.send_media_group(chat_id, media_group)


