import os
import time
import shutil
import requests
from glob import glob
from tqdm import tqdm

import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime


CODE_PATH = r'python 파일이 존재하는 파일 현재 경로'
# 해당 경로에 폴더(디렉토리)는 미리 만들어 놔야함
CCU_PATH = r'\STEAMDB_DATA\[CCU] Steam charts'
LTU_PATH = r'\STEAMDB_DATA\[LTU] Lifetime player count history'

'''
# 절대 CSV 파일을 재저장하지 마세요.
# Timestamp가 깨저서 무결성이 나빠져요.
'''

#%%
# normal setting
os.chdir(CODE_PATH)

# 파일명으로 사용 못하는 문자 제거.
def replace_nouns(text):
    replaces = (('\\', ''), ('/', ''), (':',''), ('*',''), ('?',''), 
                ('"',''), ("'",''), ('<',''), ('>',''), ('|',''))
    return reduce(lambda p, rpls: p.replace(*rpls), replaces, text)

#%%
# 스팀 DB 자체로는, 크롤링이 불가능하다. / capcha를 해킹하면 내가 해커지..

# CCU 확인 API -> 시간단위로 계속 확인해야함. 자동화 필요.
header = {"Client-ID": "API의 클라이언트 ID"}
appId = 730
game_players_url = f'https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?format=json&appid={appId}'
game_players = requests.get(game_players_url, headers=header)

print("Game name: Dota 2" + ", Player count: " + str(game_players.json()['response']['player_count']))

# 스팀 app id
appId = 570 # 수치가 정상적으로 등장하진 않음.


# 개인 정보는 확인 가능한데;; 의미가 없고. # 제 계정 트레킹입니다.
header = {"Client-ID": "API의 클라이언트 ID"}
steamId = 76561198101038016
game_players_url = f'https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/?format=json&steamids={steamId}&key=34438BC3967875E71F8CD9CED8EC0BA7'
game_players = requests.get(game_players_url, headers=header)

# 다만, 실시간 정보(쿼리 시점)만 건져진다. 누적 정보를 얻지는 못함

#%%

# 해결책이 두가지가 존재할것.
# 1. 추적할 게임들의 SteamDB페이지를 방문, 일정 주기로 해당 데이터를 업로드 / Merge하는 방법. (10일 주기)
# 2. 추적할 게임들의 CCU를 매 시간마다 쿼리, 컴퓨터를 24시간 동작(서버)시키는 방법.
# [난이도가 그렇게 높지 않기에, 갱신주기가 생각보다 길기에 1번으로 선택하여 진행함. 2번은 코딩 소요가 길고 별도 서버 마련이 필요]

def read_csv_merged(path):
    os.chdir(path) # 포인터 이동.
    line = []
    # 스팀은 기본적으로 로깅할게 두가지.
    if path.find('[STEAM]') != -1:
        file_names = glob('*.csv') # 현 위치에서 
        
        for file in file_names:
            # 시간단위가 달라서 시간으로 변경. 08시랑, 8시 동시 표기 하고 있음.
            # 근데 이 방법으로도 중복값 제거 안됨;; 그냥 바로 append하나 차이 
            temp = pd.read_csv(file)
            temp['Datetime'] = pd.to_datetime(temp['DateTime'], format='%Y-%m-%d %H:%M')
            line.append(temp)
        
        merge_df = line[0]
        
        # single files
        if len(line) <= 1:
            os.chdir(CODE_PATH) # 포인터 이동.
            return merge_df.loc[:,['DateTime','Users','In-Game']]
        
        # multiple files
        
        for idx in range(1,len(line)):
            merge_df = pd.merge(merge_df, line[idx], on = 'DateTime', how='outer')
            
            # Full join이기에 중복값 제거
            merge_df = merge_df.drop_duplicates()
            # merge_df = merge_df.groupby(level=0).last()
            
            # user열로 들어오니까.
            temp_user = pd.concat([merge_df['Users_x'], merge_df['Users_y']], axis = 1).max(axis = 1)
            temp_ingame = pd.concat([merge_df['In-Game_x'], merge_df['In-Game_y']], axis = 1).max(axis = 1)
            tempdf = pd.concat([merge_df['DateTime'], temp_user, temp_ingame], axis = 1)
            tempdf.columns = ['DateTime', 'Users', 'In-Game']
            
            merge_df = tempdf.copy()
    
    # Playtest 애들은 특이하게 User로 라벨링 된다.
    elif path.find('Playtest') != -1:
        file_names = glob('*.csv') # 현 위치에서 
        
        for file in file_names:
            # 시간단위가 달라서 시간으로 변경. 08시랑, 8시 동시 표기 하고 있음.
            # 근데 이 방법으로도 중복값 제거 안됨;; 그냥 바로 append하나 차이 
            temp = pd.read_csv(file)
            temp['Datetime'] = pd.to_datetime(temp['DateTime'], format='%Y-%m-%d %H:%M')
            line.append(temp)
        
        merge_df = line[0]
        
        # single files
        if len(line) <= 1:
            os.chdir(CODE_PATH) # 포인터 이동.
            return merge_df.loc[:,['DateTime','Users']]
        
        # multiple files
        
        for idx in range(1,len(line)):
            merge_df = pd.merge(merge_df, line[idx], on = 'DateTime', how='outer')
            
            # Full join이기에 중복값 제거
            merge_df = merge_df.drop_duplicates()
            # merge_df = merge_df.groupby(level=0).last()
            
            # user열로 들어오니까.
            temp_user = pd.concat([merge_df['Users_x'], merge_df['Users_y']], axis = 1).max(axis = 1)
            tempdf = pd.concat([merge_df['DateTime'], temp_user], axis = 1)
            tempdf.columns = ['DateTime', 'Users']
            
            merge_df = tempdf.copy()
    
    # 그 외는 Players만 가져오면 된다.
    else:
        file_names = glob('*.csv') # 현 위치에서 
        
        for file in file_names:
            # 시간단위가 달라서 시간으로 변경. 08시랑, 8시 동시 표기 하고 있음.
            temp = pd.read_csv(file)
            temp['Datetime'] = pd.to_datetime(temp['DateTime'], format='%Y-%m-%d %H:%M')
            line.append(temp)
        
        merge_df = line[0]
        
        # single files
        if len(line) <= 1:
            os.chdir(CODE_PATH) # 포인터 이동.
            return merge_df.loc[:,['DateTime','Players']]
        
        # multiple files
        
        for idx in range(1,len(line)):
            merge_df = pd.merge(merge_df, line[idx], on = 'DateTime', how='outer')
            
            # Full join이기에 중복값 제거
            merge_df = merge_df.drop_duplicates()
            
            # user열로 들어오니까.
            temp_user = pd.concat([merge_df['Players_x'], merge_df['Players_y']], axis = 1).max(axis = 1)
            tempdf = pd.concat([merge_df['DateTime'], temp_user], axis = 1)
            tempdf.columns = ['DateTime', 'Players']
            
            merge_df = tempdf.copy()
    
    os.chdir(CODE_PATH) # 포인터 이동.
    merge_df = merge_df.sort_values('DateTime')
    
    return merge_df

#%%
# 개별 Summary 실시.


# 결과 폴더 제작.
if os.path.exists('Summarise_Dataset'):
    shutil.rmtree('Summarise_Dataset', ignore_errors=True)


# 하위 폴더 제작.
os.mkdir('Summarise_Dataset')
os.mkdir('Summarise_Dataset/CCU')
os.mkdir('Summarise_Dataset/LTU')


# CCU Work
# folder filtering
ccu_folder = [i for i in os.listdir(CODE_PATH + CCU_PATH) if os.path.isdir(CODE_PATH + CCU_PATH + '/' + i)]
ccu_path = [CODE_PATH + CCU_PATH + '\\' + i for i in ccu_folder]
for folder_name in ccu_folder:
    os.mkdir('Summarise_Dataset/CCU/' + folder_name)


# CCU merge후 다시 저장.
for idx, folder in enumerate(ccu_path):
    merged_files = read_csv_merged(folder)
    merged_files.to_csv('Summarise_Dataset/CCU/' + ccu_folder[idx] + '/merged_files.csv',index = False)
    

# LTU Work
ltu_folder = [i for i in os.listdir(CODE_PATH + LTU_PATH) if os.path.isdir(CODE_PATH + LTU_PATH + '/' + i)]
ltu_path = [CODE_PATH + LTU_PATH + '\\' + i for i in ltu_folder]
for folder_name in ltu_folder:
    os.mkdir('Summarise_Dataset/LTU/' + folder_name)


# LTU merge후 다시 저장.
for idx, folder in enumerate(ltu_path):
    merged_files = read_csv_merged(folder)
    merged_files.to_csv('Summarise_Dataset/LTU/' + ccu_folder[idx] + '/merged_files.csv',index = False)
    
#%%
# summary 테이블 만들기.

ccu_df = []
ltu_df = []

# 폴더 명 찾기
os.chdir(CODE_PATH + r'\Summarise_Dataset\CCU')
ccu_folder_name = [i for i in os.listdir() if os.path.isdir(i)]

os.chdir(CODE_PATH + r'\Summarise_Dataset\LTU')
ltu_folder_name = [i for i in os.listdir() if os.path.isdir(i)]

os.chdir(CODE_PATH)

# merge CCU
for folder in ccu_folder_name:
    os.chdir(CODE_PATH + r'\Summarise_Dataset\CCU' + '\\' +  folder)
    file_names = glob('*.csv') # 현 위치에서 
    temp_df = pd.read_csv(file_names[0])
    
    if folder == '[STEAM]':
        temp_df.columns = ['DateTime', 'Steam-CCU', 'Steam-In-Game']
        ccu_df.append(temp_df)
    
    else:
        temp_df.columns = ['DateTime', folder+'-CCU']
        ccu_df.append(temp_df)

merge_temp = ccu_df[0]

if len(ccu_df) >= 1:
    for i in range(1, len(ccu_df)):
        merge_temp = pd.merge(merge_temp, ccu_df[i], on = 'DateTime', how = 'outer').groupby('DateTime').last().reset_index()


# write temp = CCU
os.chdir(CODE_PATH)
merge_temp = merge_temp.sort_values('DateTime')
merge_temp.to_csv('CCU_merged.csv', index = False)


# merge LTU
for folder in ltu_folder_name:
    os.chdir(CODE_PATH + r'\Summarise_Dataset\LTU' + '\\' +  folder)
    file_names = glob('*.csv') # 현 위치에서 
    temp_df = pd.read_csv(file_names[0])
    
    if folder == '[STEAM]':
        temp_df.columns = ['DateTime', 'Steam-LTU', 'Steam-In-Game']
        ltu_df.append(temp_df)
    
    else:
        temp_df.columns = ['DateTime', folder+'-LTU']
        ltu_df.append(temp_df)

merge_temp = ltu_df[0]

if len(ltu_df) >= 1:
    for i in range(1, len(ltu_df)):
        merge_temp = pd.merge(merge_temp, ltu_df[i], on = 'DateTime', how = 'outer').groupby('DateTime').last().reset_index()

# write temp = CCU
os.chdir(CODE_PATH)
merge_temp = merge_temp.sort_values('DateTime')
merge_temp.to_csv('LTU_merged.csv', index = False)




