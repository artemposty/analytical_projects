

---

# Automation of Advertising Campaigns

## Overview

`AC_automation.py` is a Python script designed to manage and update a MySQL database and interact with Google Sheets. The script is optimized for time-sensitive operations and includes functionality to interact with external APIs, including Telegram (via `telebot`), and Google Sheets (via `gspread`). It automates database updates based on specified logic and communicates relevant information to various platforms.

## Features

- **MySQL Database Interaction:** Connects to a MySQL database using SQLAlchemy and PyMySQL for performing updates.
- **Google Sheets Integration:** Uses the `gspread` library to read and write data to Google Sheets.
- **Telegram Bot Integration:** Sends messages via Telegram using the `telebot` library to notify users of database changes or issues.
- **Automated Updates:** Automatically updates fields in a specific database table based on defined conditions.
- **Error Handling:** Includes robust error handling, particularly for database connection issues.
- **Time Zone Adjustments:** Adjusts time by a specified offset to manage time-based logic.

## Example Output

The script produces output similar to the following:

```
EXAMPLE: ROI -6% || Lower CPC from 0.45 to 0.3
EXAMPLE: ROI 241% || Raise CPC from 1.6 to 1.7
EXAMPLE: ROI -97% || Lower CPC from 1.1 to 0.3
```

In this output:
- **ROI** indicates the return on investment percentage.
- **CPC** refers to the cost-per-click value being adjusted.

## Error Handling

If there are issues with the database connection or external APIs, the script logs the errors and sends a Telegram notification to alert the user.
