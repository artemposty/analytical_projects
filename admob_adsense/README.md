# AdMob and AdSense Table

## Overview

`admob_adsense.py` is a Python script designed to help resolve an issue with Google AdMob and BigQuery integration where revenue data from AdMob wasn't showing, preventing my team from tracking Return on Investment (ROI) on different ad campaigns in Google AdSense. 

The script automates the retrieval of campaign data from Google Ads and AdMob, calculates ROI for each campaign, and uploads the results to a Google Sheet. This provides an easy-to-use solution for evaluating the performance of Google Ad campaigns by combining cost data from Google Ads with revenue data from AdMob.

## Key Features

- **Google Ads Data Collection**: Retrieves campaign IDs, names, and costs from Google Ads for specified campaigns over a defined date range.
- **AdMob Revenue Retrieval**: Connects to AdMob API to pull estimated earnings and eCPM metrics, mapped to the corresponding campaigns.
- **ROI Calculation**: Combines data from Google Ads and AdMob, calculates ROI, and organizes it for easy review.
- **Google Sheets Integration**: Exports the final ROI report to a Google Sheet for further analysis and sharing.


## Output

- **Google Sheets Update**: The ROI, along with campaign costs, revenues, and eCPM, is exported to a specified Google Sheet.
  
- **ROI Calculation**: For each campaign, ROI is calculated using the formula:
  ```python
  ROI = (Revenue - Cost) / Cost
  ```

## Example

Below is an example of the output DataFrame before exporting to Google Sheets:

| COUNTRY | Cost | Revenue | ECPM | ROI  |
|---------|------|---------|------|------|
| US      | 1000 | 1200    | 5.2  | 0.20 |
