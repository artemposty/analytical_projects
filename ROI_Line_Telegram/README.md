# Accounts Advertising Performance Monitoring Bot

This Python script automates the process of generating and sending performance graphs (Revenue, Spend, and ROI) from advertising accounts to a designated Telegram channel. The bot sends reports every hour, displaying critical metrics in a visual format for easy analysis.

## Key Features

- **Automated Reports:** The bot generates graphs for each account and sends them to a Telegram channel every hour between 8 AM and 11 PM.
- **Graphical Data:** Visualize **revenue**, **spend**, and **ROI** for advertising accounts using Matplotlib.
- **Customizable**: Easily add or modify accounts via a text file, and filter data by custom time ranges.
- **Error Logging:** All activity, including errors, is logged for easy troubleshooting.

## Example Outputs

Here are some example outputs of the script:

- [Example Telegram Message 1](https://github.com/artemposty/yandex_ads/blob/main/images/example1.png)
- [Example Telegram Message 2](https://github.com/artemposty/yandex_ads/blob/main/images/example2.png)
- [Log File Example](https://github.com/artemposty/yandex_ads/blob/main/images/example3.png)

## How It Works

1. **Data Collection:** The script connects to a MySQL database for each advertising account to retrieve hourly revenue and spend data.
2. **Graph Generation:** Using Matplotlib, the script generates ROI graphs for each account, highlighting areas where spend exceeds revenue (ROI < 0) and vice versa (ROI > 0).
3. **Telegram Delivery:** The generated graphs are sent in a batch to a predefined Telegram chat using the Telegram Bot API.
4. **Scheduled Execution:** The script is designed to run every hour via a cron job, providing frequent and up-to-date reports.

## Prerequisites

- **Python 3.x**
- **MySQL Database:** 
- **Telegram Bot:** 
- Used Python Libraries:
  - `matplotlib`
  - `pandas`
  - `telebot`
  - `numpy`
  - `sqlalchemy`
  - `pymysql`


