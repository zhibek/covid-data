import requests
import csv
from requests.utils import quote
from datetime import datetime, timedelta

SOURCE_HOST = 'https://api.coronavirus.data.gov.uk'
SOURCE_URL = SOURCE_HOST \
    + '/v1/data' \
    + '?filters=areaType=nhsRegion' \
    + '&structure=' \
    + quote('{"areaName":"areaName","areaCode":"areaCode","date":"date","newAdmissions":"newAdmissions"}') \
    + '&format=json'
TARGET_PATH = '../data/hospital_admissions.csv'
DAY_DELTA = timedelta(days=1)
DATE_FORMAT = '%Y-%m-%d'
AREAS = [
    'London',
    'South East',
    'South West',
    'East of England',
    'Midlands',
    'North West',
    'North East and Yorkshire',
]

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
    count = item['newAdmissions']

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

    for area_name in AREAS:
        area_data = target_data[area_name]
        line = [
            area_name,
        ]
        date_iterate = first_date
        while date_iterate <= last_date:
            target_date = date_iterate.strftime(DATE_FORMAT)
            date_iterate += DAY_DELTA
            cases = 0
            if target_date in area_data:
                cases = area_data[target_date]
            line.append(cases)

        writer.writerow(line)

print('Process complete!')
