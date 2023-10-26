import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:admin@localhost:5432/streetfighter6')
                    
with engine.begin() as conn:
    query = text('''SELECT distinct replay_id, p1_char, p2_char, p1_result, p2_result FROM matches
                 where p1_mr > 0 or p2_mr > 0
                 ;''')
    df = pd.read_sql_query(query, conn)

#print(matches_df1)

df1 = df.rename(columns={'p1_char':'p2_char',
                                          'p2_char':'p1_char',
                                          'p1_result':'p2_result',
                                          'p2_result':'p1_result'
                                          })

df2 = pd.concat([df,df1])

df3 = df2.groupby(['p1_char','p2_char']).sum()

df3['p1_char_pct'] = df3['p1_result'] / (df3['p1_result']+df3['p2_result'])
df3['p2_char_pct'] = df3['p2_result'] / (df3['p1_result']+df3['p2_result'])

print(df3)
df3.to_csv('head2headtest.csv',encoding='UTF-8')


