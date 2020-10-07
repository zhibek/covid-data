import requests
import csv
from requests.utils import quote
from datetime import datetime, timedelta

SOURCE_HOST = 'https://api.coronavirus.data.gov.uk'
SOURCE_URL = SOURCE_HOST \
    + '/v1/data' \
    + '?filters=areaType=region' \
    + '&structure=' \
    + quote('{"areaName":"areaName","date":"date","newCasesByPublishDate":"newCasesByPublishDate"}') \
    + '&format=json'

TARGET_PATH = '../data/cases_by_published.csv'
DAY_DELTA = timedelta(days=1)
DATE_FORMAT = '%Y-%m-%d'
AREAS = {
    'London': ['London'],
    'South East': ['South East'],
    'South West': ['South West'],
    'East of England': ['East of England'],
    'Midlands': ['East Midlands', 'West Midlands'],
    'North West': ['North West'],
    'North East and Yorkshire': ['North East', 'Yorkshire and The Humber'],
}

source_url = SOURCE_URL
source_data = []
while True:
    source_result = requests.get(source_url)
    source_json = source_result.json()
    source_data = source_data + source_json['data']
    if source_json['pagination']['next']:
        source_url = SOURCE_HOST + source_json['pagination']['next']
    else:
        break

target_data = {}
first_date = None
last_date = None
for item in source_data:
    area_name = item['areaName']
    item_date = item['date']
    count = item['newCasesByPublishDate']

    if not first_date or item_date < first_date:
        first_date = item_date
    if not last_date or item_date > last_date:
        last_date = item_date

    if area_name not in target_data:
        target_data[area_name] = {}
    target_data[area_name][item_date] = count

first_date = datetime.strptime(first_date, DATE_FORMAT)
last_date = datetime.strptime(last_date, DATE_FORMAT)

with open(TARGET_PATH, mode='w') as target_file:
    writer = csv.writer(target_file)
    target_headers = ['area_name']
    date_iterate = first_date
    while date_iterate <= last_date:
        target_date = date_iterate.strftime(DATE_FORMAT)
        date_iterate += DAY_DELTA
        target_headers.append(target_date)
    writer.writerow(target_headers)

    for nhs_region in AREAS:
        line = [
            nhs_region,
        ]
        date_iterate = first_date
        while date_iterate <= last_date:
            target_date = date_iterate.strftime(DATE_FORMAT)
            date_iterate += DAY_DELTA
            cases = 0
            for region in AREAS[nhs_region]:
                if target_date in target_data[region]:
                    cases = cases + target_data[region][target_date]
            line.append(cases)

        writer.writerow(line)

print('Process complete!')
