import requests

SOURCE_URL = 'https://coronavirus.data.gov.uk/downloads/msoa_data/MSOAs_latest.csv'
TARGET_PATH = '../data/cases_at_msoa.csv'

source_result = requests.get(SOURCE_URL)
source_data = source_result.content.decode('utf-8')

with open(TARGET_PATH, mode='w') as target_file:
    target_file.write(source_data)

print('Process complete!')
