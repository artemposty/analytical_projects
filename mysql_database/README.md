# Yandex Games Ad Campaign Data Processing and Upload Script

This Python script was specifically designed to automate the process of extracting advertising performance data for **Yandex Games Ad Campaigns**. It processes data from multiple sources (Yandex Direct, Metrika, RSA) and uploads it to a MySQL database. The script calculates key performance indicators (KPIs) such as revenue, cost, clicks, ROI, and more, enabling detailed analysis of campaign performance.

## Key Features

- **Data Extraction**: Extracts relevant performance data for Yandex Games ad campaigns from JSON API responses.
- **Data Transformation**: Processes the data into a DataFrame, calculating key metrics like ROI, ARPU, CPC, and more.
- **Database Upload**: Uploads the processed data into a MySQL database for further analysis.
- **Error Handling**: Gracefully handles missing keys and errors during data extraction and upload processes.
  
Used Python Libraries:
- `pandas`
- `sqlalchemy`
- `pymysql`
- `requests`

## How It Works

1. **Current Time Setup**: The script sets the current time based on the server's time zone to accurately timestamp data uploads.
2. **API Requests**: Uses API keys to retrieve data from Yandex Direct, Metrika, and RSA APIs.
3. **Data Parsing**: Extracts nested fields from the API JSON response, processing metrics like revenue, spend, clicks, and sessions.
4. **Custom Metrics Calculation**: Calculates several KPIs, including:
   - **ROI**: Return on Investment
   - **ARPU**: Average Revenue Per User
   - **CPC**: Cost Per Click
   - **Organic**: Percentage of organic traffic
   - **Net Profit**
5. **Database Upload**: Uploads the transformed data to a MySQL database in tables specific to each client.
6. **Error Handling**: Logs any errors during the process, making debugging easier.

## Usage

The script will iterate through each Yandex Games account listed in the file, retrieve data via API, process it, and upload it to the database.

## Data Transformation

The script processes data into several key columns, including:
- **Game Name**: Cleaned game title from the API response.
- **Sessions, Clicks, Shows**: Basic traffic metrics.
- **Revenue & Cost**: Financial data for each campaign.
- **ROI**: Return on Investment, calculated as `(Revenue - Cost) / Cost`.
- **CPC**: Cost per click, calculated as `Cost / Clicks`.
- **ARPU**: Average Revenue Per User.
- **Net Profit**: `Revenue - Cost`.
- **URL**: A dynamic link to the game’s direct console.

## Error Handling and Logging

The script includes error handling to catch and log issues such as:
- Missing data fields in the JSON responses.
- API connection issues.
- Database upload errors.
