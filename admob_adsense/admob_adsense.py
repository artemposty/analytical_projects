from google.ads.googleads.client import GoogleAdsClient
import pandas as pd
from datetime import datetime, timedelta
import pycountry
import os
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import datetime as dt


now = datetime.now()

current_day = now.day
current_month = now.month

# GOOGLE ADS

def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService", version="v17")

    # Calculate the date 30 days ago
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = '2024-06-01'

    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY campaign.id"""

    # Issues a search request using streaming.
    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    # Initialize lists to store the campaign data
    campaign_ids = []
    campaign_names = []
    campaign_costs = []

    for batch in stream:
        for row in batch.results:
            campaign_ids.append(row.campaign.id)
            campaign_names.append(row.campaign.name)
            campaign_costs.append(row.metrics.cost_micros / 1_000_000)  # Convert from micros to standard currency units

    # Create a DataFrame and store it in a variable
    df = pd.DataFrame({
        'Campaign ID': campaign_ids,
        'Campaign Name': campaign_names,
        'Cost': campaign_costs
    })

    return df

if __name__ == "__main__":
    # Initialize the Google Ads client
    client = GoogleAdsClient.load_from_storage("/Users/artempostavy/Downloads/google-ads.yaml")

    # Specify the customer ID
    customer_id = "4205785692"

    # Call the main function and store the DataFrame in a variable
    df = main(client, customer_id)


df = df[df['Campaign Name'].str.startswith('StreetFight')]
df['Campaign Name'] = df['Campaign Name'].str[12:14]
df = df[['Campaign Name', 'Cost']]
df = df.groupby(['Campaign Name']).sum()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Путь к файлу credentials.json
CREDENTIALS_FILE = ''
TOKEN_FILE = ''
SCOPES = ['']

def authenticate():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=8080, prompt='consent', authorization_prompt_message='Please visit this URL: {url}')
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return creds

def list_accounts(service):
    accounts = service.accounts().list().execute()
    return accounts

def get_admob_revenue(service, account_id):
    request = {
        "reportSpec": {
            "dateRange": {
                "startDate": {"year": 2024, "month": 6, "day": 1},
                "endDate": {"year": 2024, "month": 12, "day": 31},
            },
            "dimensions": ["COUNTRY"],
            "metrics": ["ESTIMATED_EARNINGS", "OBSERVED_ECPM"],
            "dimensionFilters": [
                {
                    "dimension": "APP",
                    "matchesAny": {"values": [""]}
                }
            ],
            "sortConditions": [
                {"dimension": "COUNTRY", "order": "ASCENDING"}
            ],
            "localizationSettings": {
                "currencyCode": "USD",
                "languageCode": "en-US"
            }
        }
    }

    response = service.accounts().mediationReport().generate(parent=f'accounts/{account_id}', body=request).execute()
    return response

def parse_revenue_data(revenue_data):
    data = []

    for entry in revenue_data:
        if 'row' in entry:
            row = entry['row']
            date = row['dimensionValues']['COUNTRY']['value']
            earnings = row['metricValues']['ESTIMATED_EARNINGS']['microsValue']
            ecpm = row['metricValues']['OBSERVED_ECPM']['microsValue']
            earnings_dollars = int(earnings) / 1_000_000
            ecpm_dollars = int(ecpm) / 1_000_000
            data.append([date, earnings_dollars, ecpm_dollars])

    df = pd.DataFrame(data, columns=['COUNTRY', 'Estimated Earnings (USD)', 'ECPM (USD)'])
    return df

credentials = authenticate()
service = build('admob', 'v1', credentials=credentials)

accounts = list_accounts(service)
account_id = accounts['account'][0]['publisherId']

revenue_data = get_admob_revenue(service, account_id)

df_revenue = parse_revenue_data(revenue_data)
df_revenue.sort_values(by='Estimated Earnings (USD)', ascending=False)

df = pd.merge(df, df_revenue, left_on='Campaign Name', right_on='COUNTRY', how='inner')


def get_country_name(code):
    try:
        return pycountry.countries.get(alpha_2=code).name
    except AttributeError:
        return None

df['COUNTRY'] = df['COUNTRY'].apply(get_country_name)

df.rename(columns={'Estimated Earnings (USD)': 'Revenue', 'ECPM (USD)':'ECPM'}, inplace=True)
df['ROI'] = ((df['Revenue'] - df['Cost']) / df['Cost']).round(2)
df = df[['COUNTRY', 'Cost', 'Revenue', 'ECPM', 'ROI']]
df = df[(df['Cost'] > 0)]
df = df.sort_values(by='Cost', ascending=False)
df['Cost'] = df['Cost'].round(0)
df['Revenue'] = df['Revenue'].round(0)
df['ECPM'] = df['ECPM'].round(0)
df['ROI'] = df['ROI'].round(2)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = ServiceAccountCredentials.from_service_account_file("", scopes=scopes)
client = gspread.authorize(creds)

sheet_id = ""
workbook = client.open_by_key(sheet_id)

worksheet = workbook.worksheet("Google_РК")
df_values = [df.columns.tolist()] + df.values.tolist()

cell_range = f'B1:{chr(ord("B") + len(df.columns) - 1)}{len(df_values)}'


column_range = 'B:F'
cells_to_clear = worksheet.range(column_range)
for cell in cells_to_clear:
    cell.value = ''
worksheet.update_cells(cells_to_clear)
worksheet.update(df_values, cell_range)

if worksheet:
    now = dt.datetime.now()
    current_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update_cell(2, 1, current_datetime)





