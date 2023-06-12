import os
import time
import shutil
import requests
import schedule
from glob import glob
from tqdm import tqdm

import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime

#%%

twitch_game_id = {'Space Gears' : 185794439,
                  'VALORANT' : 516575,
                  'AGE OF EMPIRES III' : 7830,
                  'AGE OF EMPIRES II' : 13389,
                  'AGE OF EMPIRES' : 9623,
                  'AGE OF WONDERS 4' : 1908293093,
                  'BATTLETECH' : 490757,
                  'CRUSADER KINGS III' : 514888,
                  'Dota2' : 29595,
                  'LEAGUE OF LEGENDS' : 21779,
                  'Europa Universalis IV' : 67584,
                  'Hearts of Iron IV' : 459327,
                  'MECHABELLUM' : 515881069,
                  'Iron Harvest' : 498423,
                  "SID MEIER CIVILIZATION IV" : 15084,
                  'SID MEIER CIVILIZATION V' : 27103,
                  'SMITE' : 32507,
                  'Starship Troopers' : 10297,
                  'Stellaris' : 491572,
                  'SUPREME COMMANDER FORGED ALLIANCE' : 16553,
                  'TOTAL WAR: WARHAMMER III' : 1913410799,
                  'JUST CHATTING' : 509658,
                  'STARCRAFT' : 11989,
                  'STARCRAFT II' : 490422
                  }

# 파일명으로 사용 못하는 문자 제거.
def replace_nouns(text):
    replaces = (('\\', ''), ('/', ''), (':',''), ('*',''), ('?',''), 
                ('"',''), ("'",''), ('<',''), ('>',''), ('|',''))
    return reduce(lambda p, rpls: p.replace(*rpls), replaces, text)

#%%

CODE_PATH = r'python 파일이 존재하는 파일 현재 경로'


#%%

os.chdir(CODE_PATH)
file_names = glob('Twitch_Dataset/*.csv')
        
# 기존 파일 제거 # - 기존 데이터 지울거라 이제 위험함.
'''
if os.path.exists('Summarise_Twitch_Dataset'):
    shutil.rmtree('Summarise_Twitch_Dataset', ignore_errors=True)
'''

# 경로에 폴더 만들기.
if not os.path.exists('Summarise_Twitch_Dataset'):
    os.mkdir('Summarise_Twitch_Dataset')

merge_df = {}

# 데이터 읽어서 형태로 제작.
for file in tqdm(file_names):
    df = pd.read_csv(file)
    
    for game_name, game_id in twitch_game_id.items():
        query_df = df[df['game_id']==game_id]
        
        tag_list = []
        for tag in query_df['tags'].tolist():
            if tag is not np.nan:
                tag = eval(tag)
            else:
                tag = []
            tag_list.extend(tag)
        
        
        input_shape = [file.split('\\')[1].replace('.csv',''),
                       game_name,
                       query_df['user_id'].count(),
                       df['user_id'].count(),
                       query_df['viewer_count'].sum(),
                       df['viewer_count'].sum(),
                       str(query_df['language'].value_counts().to_json()),
                       str(query_df['user_login'].to_list()),
                       str(query_df['user_name'].to_list()),
                       str(sorted(list(set(tag_list)))),
                       ]
        
        temp_df = pd.DataFrame(input_shape).T
        temp_df.columns = ['DATETIME', 'game_name', 'broadcaster_cnt', 'total_broadcaster_cnt', 'viewer_cnt', 'Total_twitch_viewer', 'language', 'user_ids', 'user_names', 'tags_unique']
        
        if game_name in merge_df:
            merge_df[game_name].append(temp_df)
        else:
            merge_df[game_name] = [temp_df]

# merge하고 데이터 프레임 각 폴더에 저장.
for game_name, df in merge_df.items():
    merge_df[game_name] = pd.concat(df, axis = 0)
    
    if not os.path.exists('Summarise_Twitch_Dataset/'+replace_nouns(game_name)):
        os.mkdir('Summarise_Twitch_Dataset/'+replace_nouns(game_name))
    
    
    merge_df[game_name].to_csv('Summarise_Twitch_Dataset/'+replace_nouns(game_name)+'/Twitch_Summary_'+replace_nouns(game_name)+'.csv', index = False)
    
# 머지 해서 저장할 수 있는 형태여야함. (기존 데이터 지워질거 고려.)
    
