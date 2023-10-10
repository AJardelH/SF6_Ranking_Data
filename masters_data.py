import pandas as pd

#read CSV into dataframe
df = pd.read_csv('csv_file_here.csv',encoding='UTF-8')

#rename columns to simplify
df = df.rename(columns=
               {'fighter_banner_info.personal_info.fighter_id': 'player',
               'fighter_banner_info.personal_info.platform_name': 'platform',
               'character_name': 'character',
               'fighter_banner_info.home_name': 'home_region'}
               )

#attempt to drop dead rated 1500 rated accounts
#inherant risk some active 1500 rated accounts will be lost
df.drop(df.loc[df['rating']==1500].index, inplace=True)

#sort by master rating descending & keep 'main' character
df = df.sort_values("rating", ascending=False)
df = df.drop_duplicates(subset='player',keep='first')

#print(df)
df.to_csv('cleaned_masters_data.csv')




