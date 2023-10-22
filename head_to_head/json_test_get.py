import requests
from bs4 import BeautifulSoup
import traceback
import json
import pandas as pd
from cookies_headers import cookies, headers

url = 'https://www.streetfighter.com/6/buckler/profile/1764931801/battlelog/rank?page=1'

response = requests.get(f'{url}', cookies=cookies(), headers=headers()).text

soup = BeautifulSoup(response, 'html.parser')

# Find the script tag with id "__NEXT_DATA__"
script_tag = soup.find('script', {"id": "__NEXT_DATA__"})

if script_tag:
    # Extract the JSON data from the script tag
    json_data = json.loads(script_tag.text)

    # Access the specific data you need from the JSON object
    replay_list = json_data['props']['pageProps']['replay_list']

    # Convert replay_list into a DataFrame
    df = pd.json_normalize(replay_list)

    # Display the DataFrame
    print(df)
    df.to_csv('test.csv')
