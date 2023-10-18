import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import math
import time
from cookies_headers import cookies, headers
import traceback

def get_sf6_ranks():

    #set page no param to 1
    page_no = 1

    #league rank parameter for iterating
    #range is 1 through 37 for Rookie 1 through Master (rookie1 = 1, master = 36)
    league_rank = range(36,37)

    #create empty dataframe
    df = pd.DataFrame()

    #set number of max pages here if not using player count
    max_pages = 200

    url = 'https://www.streetfighter.com/6/buckler/ranking/league'

    for rank in league_rank:
        while page_no <= max_pages:
            params = {
            'page': page_no,'league_rank':rank
            }

            #use buckler/ranking/master for master rating otherwise sorts master rank by league points not rating, json changes to 'rating'
            if rank == 36:
                url = 'https://www.streetfighter.com/6/buckler/ranking/master'

            #cookies and headers required / log in
            response = requests.get(f'{url}', params=params, cookies=cookies(), headers=headers()).text

            soup = BeautifulSoup(response, 'html.parser')

            #uncomment below and import math to get all players for rank
            #gets total player count of rank from page, divides by 20 and rounds up for max number of pages per rank
            try:
                player_count = int(soup.find('span',{'class':'ranking_ranking_now__last__TghLM'}).text.replace('/ ',''))
                max_pages = math.ceil(player_count/20)
            except Exception:
                    traceback.print_exc()
                    #dump incomplete csv with page no 
                    print(f'dumping incomplete .csv')
                    df.to_csv(f'csv_name_here_{rank}_incomplete_{page_no}.csv',index=False)
                    break
            
            
            #checks on players / pages count
            #print(player_count)
            #print(max_pages)
        
            #find script tag containing the JSON data we need
            script_tag = soup.find('script',{'id':'__NEXT_DATA__'})
            if script_tag is not None:
                try:
                    json_blob = json.loads(script_tag.get_text())
                    #JSON is slightly different for /buckler/ranking/master which orders by master rating and not by league points
                    #'master_rating' is just called 'rating' in this data
                    if rank == 36:
                        page_data = json_blob['props']['pageProps']['master_rating_ranking']['ranking_fighter_list']
                        df = pd.concat([df, pd.json_normalize(page_data)],ignore_index=True)
                        df = df.loc[:,['fighter_banner_info.personal_info.fighter_id',
                                    'fighter_banner_info.personal_info.platform_name',
                                    'character_name',
                                    'league_point',
                                    'league_rank',
                                    'rating',
                                    'fighter_banner_info.home_name'
                            ]]
                                                
                    else:
                        page_data = json_blob['props']['pageProps']['league_point_ranking']['ranking_fighter_list']
                        df = pd.concat([df, pd.json_normalize(page_data)],ignore_index=True)
                        df = df.loc[:,['fighter_banner_info.personal_info.fighter_id',
                                    'fighter_banner_info.personal_info.platform_name',
                                    'character_name',
                                    'league_point',
                                    'league_rank',
                                    'fighter_banner_info.home_name'
                            ]]
                        
                    print(f'page {page_no}/{max_pages} of league {rank} complete')
                    page_no += 1
                except Exception:
                    traceback.print_exc()
                    #dump incomplete csv with page no 
                    print(f'dumping incomplete .csv')
                    df.to_csv(f'csv_name_here_{rank}_incomplete_{page_no}.csv',index=False)
                    break
                
                          
            #export csv #optional rank variable input
            df.to_csv(f'csv_name_here_{rank}.csv',index=False) 
            
            #wait x per page reduce requests/s if wanted
            time.sleep(2)
        
        #print df to check
        #print(df)

        #clear out dataframe for next rank reset page to 1 to loop through next ranks
        df = pd.DataFrame()
        page_no = 1

if __name__ == "__main__":
    get_sf6_ranks()
