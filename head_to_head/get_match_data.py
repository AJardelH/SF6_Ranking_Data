import requests
from bs4 import BeautifulSoup
import time
import json
import pandas as pd
from cookies_headers import cookies, headers
import sqlalchemy as db
from sqlalchemy import create_engine, text
from requests.exceptions import RequestException

engine = create_engine('postgresql://postgres:admin@localhost:5432/streetfighter6')

def check_loss(round_results):
    return round_results.count(0) >= 2

def get_match_data():
    with engine.begin() as conn:
        query = text('''SELECT id, player_id FROM players
                      LIMIT 500
                      OFFSET 3500;''')
        
        short_id_df = pd.read_sql_query(query, conn)
        max_pages = 10
        df_rename_dict = {
            'player1_info.player.short_id': 'p1_id',
            'player1_info.master_rating': 'p1_mr',
            'player1_info.character_name': 'p1_char',
            'player2_info.player.short_id': 'p2_id',
            'player2_info.master_rating': 'p2_mr',
            'player2_info.character_name': 'p2_char'
        }

        for index, row in short_id_df.iterrows():
            short_id = row['player_id']
            id = row['id']
            page = 1

            while page <= max_pages:
                url = f'https://www.streetfighter.com/6/buckler/profile/{short_id}/battlelog/rank?page={page}'
                response = requests.get(url, cookies=cookies(), headers=headers()).text
                soup = BeautifulSoup(response, 'html.parser')
                df = pd.DataFrame()

                next_disabled_elements = soup.find_all(class_="next disabled")
                if next_disabled_elements:
                    print(f'No new next page. Stopped at id {id} (short_id: {short_id} page no. {page})')
                    page = 1  # Reset page no
                    time.sleep(2)
                    break  # Move to the next short_id

                script_tag = soup.find('script', {"id": "__NEXT_DATA__"})

                if script_tag is not None:
                    try:
                        json_data = json.loads(script_tag.text)
                        replay_list = json_data['props']['pageProps']['replay_list']
                        df = pd.concat([df, pd.json_normalize(replay_list)], ignore_index=True)
                        df = df.loc[:, ['replay_id',
                                        'player1_info.player.short_id',
                                        'player1_info.master_rating',
                                        'player1_info.character_name',
                                        'player1_info.round_results',
                                        'player2_info.player.short_id',
                                        'player2_info.master_rating',
                                        'player2_info.character_name',
                                        'player2_info.round_results']]

                        df['p1_result'] = df['player1_info.round_results'].apply(check_loss)
                        df['p2_result'] = ~df['p1_result']

                        df.rename(columns=df_rename_dict, inplace=True)

                        df = df.drop(columns=(['player1_info.round_results', 'player2_info.round_results']))

                        df.to_sql('matches', con=engine, if_exists='append', index=False, schema='public',
                                  dtype={'replay_id': db.types.Text(),
                                         'p1_id': db.types.BIGINT(),
                                         'p1_mr': db.types.Integer(),
                                         'p1_char': db.types.Text(),
                                         'p2_id': db.types.BIGINT(),
                                         'p2_mr': db.types.Integer(),
                                         'p2_char': db.types.Text(),
                                         'p1_result': db.types.Boolean(),
                                         'p2_result': db.types.Boolean()
                                         })
                        print(f'index {id}. Player {short_id} page number {page}')
                        print(df)

                    except RequestException as network_exception:
                        print(f'Network error processing id {id} (short_id: {short_id}): {str(network_exception)}')
                        raise network_exception

                    except Exception as e:
                        print(f'Error processing id {id} player {short_id}): {str(e)}')
                        # Move to the next short_id on exception
                        break
                page = page + 1
                #time.sleep(2)

if __name__ == "__main__":
    get_match_data()
