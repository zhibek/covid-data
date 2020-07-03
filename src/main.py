import io
import csv
from datetime import date, timedelta
import urllib.request
from sys import stdout

# SOURCE_URL = 'https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv'
SOURCE_URL = 'https://c19downloads.azureedge.net/downloads/csv/coronavirus-cases_latest.csv'
TARGET_PATH = '../data/target.csv'
START_DATE = date(2020, 1, 30)
END_DATE = date.today()
DAY_DELTA = timedelta(days=1)

with urllib.request.urlopen(SOURCE_URL) as source_request:
    source_response = source_request.read().decode('utf-8')
    source_raw = io.StringIO(source_response)

source_data = csv.reader(source_raw)

source_headers = None
target_data = {}
for row in source_data:
    if source_headers is None:
        source_headers = row
    else:
        area_name = row[0]
        area_code = row[1]
        area_type = row[2]
        date = row[3]
        cases_raw = row[4]
        cases = 0
        if cases_raw is not None and cases_raw.strip() != '':
            cases = int(float(cases_raw))

        if area_code not in target_data:
            target_data[area_code] = {
                'meta': {
                    'area_code': area_code,
                    'area_type': area_type,
                    'area_name': area_name,
                },
                'timeseries': {},
            }

        target_data[area_code]['timeseries'][date] = cases

with open(TARGET_PATH, mode='w') as target_file:
    writer = csv.writer(target_file)
    target_headers = ['area_name', 'area_code', 'area_type']
    date_iterate = START_DATE
    while date_iterate <= END_DATE:
        date = date_iterate.strftime("%Y-%m-%d")
        date_iterate += DAY_DELTA
        target_headers.append(date)
    writer.writerow(target_headers)

    for area_code in target_data:
        item = target_data[area_code]
        line = [
            item['meta']['area_name'],
            item['meta']['area_code'],
            item['meta']['area_type'],
        ]
        date_iterate = START_DATE
        while date_iterate <= END_DATE:
            date = date_iterate.strftime("%Y-%m-%d")
            date_iterate += DAY_DELTA
            cases = 0
            if date in item['timeseries']:
                cases = item['timeseries'][date]
            line.append(cases)

        writer.writerow(line)
