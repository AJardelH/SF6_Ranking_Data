import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import math
import traceback
from sqlalchemy import create_engine
import sqlalchemy as db
from cookies_headers import cookies, headers
import time

def get_short_id():
    engine = create_engine('postgresql://postgres:admin@localhost:5432/streetfighter6')

    url = 'https://www.streetfighter.com/6/buckler/ranking/master'

    # Set page no param to 1
    page_no = 501

    # Create an empty dataframe
    df = pd.DataFrame()

    # Set the number of max pages here if not using player count
    max_pages = 1000

    while page_no <= max_pages:
        params = {
            'page': page_no
        }

        # Cookies and headers required / log in
        response = requests.get(f'{url}', params=params, cookies=cookies(), headers=headers()).text

        soup = BeautifulSoup(response, 'html.parser')

        # Uncomment below and import math to get all players for rank
        # Gets the total player count of rank from the page, divides by 20, and rounds up for max number of pages per rank
        # try:
        #     player_count = int(soup.find('span',{'class':'ranking_ranking_now__last__TghLM'}).text.replace('/ ',''))
        #     max_pages = math.ceil(player_count/20)
        # except Exception:
        #     df.drop_duplicates(subset='fighter_banner_info.personal_info.short_id', keep='first', inplace=True)
        #     df.rename(columns={'fighter_banner_info.personal_info.short_id': 'player_id'}, inplace=True)
        #     df.to_csv('test_sql.csv')
        #     print(df)
        #     df.to_sql('players', engine, if_exists='replace', index=False)
        #     traceback.print_exc()

        # Checks on players/pages count
        # print(player_count)
        # print(max_pages)

        # Find the script tag containing the JSON data we need
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if script_tag is not None:
            try:
                json_blob = json.loads(script_tag.get_text())
                page_data = json_blob['props']['pageProps']['master_rating_ranking']['ranking_fighter_list']
                df = pd.concat([df, pd.json_normalize(page_data)], ignore_index=True)
                df = df.loc[:, ['fighter_banner_info.personal_info.short_id']]

                print(f'page {page_no}/{max_pages} complete')
                page_no += 1

            except Exception:
                df.drop_duplicates(subset='fighter_banner_info.personal_info.short_id', keep='first', inplace=True)
                df.rename(columns={'fighter_banner_info.personal_info.short_id': 'player_id'}, inplace=True)
                df.to_csv('error_dump.csv')
                df.to_sql('players', engine, if_exists='append', index=False)
                traceback.print_exc()
                break

        # Wait x seconds per page to reduce requests per second if wanted
        #time.sleep(5)

    # Print df to check

    df.drop_duplicates(subset='fighter_banner_info.personal_info.short_id', keep='first', inplace=True)
    df.rename(columns={'fighter_banner_info.personal_info.short_id': 'player_id'}, inplace=True)
    # print(df.dtypes)
    # df.to_csv('test.csv')
    print(df)
    df.to_sql('players', engine, if_exists='append', index=False)

if __name__ == "__main__":
    get_short_id()
