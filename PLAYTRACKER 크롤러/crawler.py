import os
import time
import shutil
from glob import glob
from tqdm import tqdm

import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

CODE_PATH = r'python 파일이 존재하는 파일 현재 경로'

PLAYTRACKER_ID = '플레이 트레커 ID'
PLAYTRACKER_PW = '플레이 트레커 PW'
PLAYTRACKER_PAGE_PATH = 'https://playtracker.net/insight/graphs/'

# Json 다운로드 대기시간.
DOWNLOAD_WAIT = 5

#%%
# normal setting
os.chdir(CODE_PATH)

# 파일 다운로드 경로 변경
op = Options()
op.add_experimental_option('prefs', {'download.default_directory': CODE_PATH + r'\temp_download'})

driver = webdriver.Chrome('chromedriver.exe', options = op)
driver.implicitly_wait(3)


# 템프 폴더 내 파일 제거 - 폴더 없으면 에러. 미사용 폴더.
'''
def temp_remove():
    try:
        for f in os.listdir(CODE_PATH + r'\temp_download'):
            os.remove(os.path.join(CODE_PATH + r'\temp_download', f))
    except:
        print('there is no files/no folder')
'''

# 파일명으로 사용 못하는 문자 제거.
def replace_nouns(text):
    replaces = (('\\', ''), ('/', ''), (':',''), ('*',''), ('?',''), 
                ('"',''), ("'",''), ('<',''), ('>',''), ('|',''))
    return reduce(lambda p, rpls: p.replace(*rpls), replaces, text)



#%%
# Playtracker의 로그인 페이지에 접근한다
driver.get('https://playtracker.net/dashboard/')
driver.find_element(By.XPATH, '//*[@id="main-container"]/div[1]/div[2]/div/div/form/input[1]').send_keys(PLAYTRACKER_ID)
driver.find_element(By.XPATH, '//*[@id="main-container"]/div[1]/div[2]/div/div/form/input[2]').send_keys(PLAYTRACKER_PW)
driver.find_element(By.XPATH, '//*[@id="main-container"]/div[1]/div[2]/div/div/form/input[3]').click()


crawl_target = {'Age of Empires IV' : 68191,
                'Age of Wonders 4' : 85235,
                'BATTLETECH' : 13601,
                'Crusader Kings III' : 59662,
                'Company of Heroes 2' : 872,
                'Total War: SHOGUN 2' : 5514,
                'Total War: WARHAMMER III' : 73364,
                'Hearts of Iron IV' : 2796,
                'Command & Conquer™ Remastered Collection' : 59976,
                'Anno 1800' : 35674,
                'Europa Universalis IV' : 6100,
                'SMITE' : 1500,
                'Mechabellum' : 85357,
                'Sid Meier Civilization IV' : 1611,
                'Stellaris' : 890
                }


# 추후 위에서 고치고 일단 여기만 수정하면 항상 사용 가능.
for game_name, game_id in tqdm(crawl_target.items()):
    driver.implicitly_wait(3)
    print(game_name)
    driver.get(PLAYTRACKER_PAGE_PATH + str(game_id))

    # 현재 페이지 전체 끌고 오고
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # 해당 제목 맞는지 확인하고
    GAMENAME = replace_nouns(soup.select_one('#swup > div > div > div:nth-child(1) > div.third.insight-sidebar.sidebar-lift.sidebar-lift-insight.flex-responsive > div.white-space.center-text > h1').get_text())
    print(GAMENAME)


    # 데이터 수집 후 저장.
    for idx, txt in enumerate(['concurrent','popularity','recent','active']):
        if str(txt).find('concurrent') != -1:
            with open('data_saved/CCU/' + GAMENAME + '.json','w') as f:
                f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
        if str(txt).find('popularity') != -1:
            with open('data_saved/popularity/' + GAMENAME + '.json','w') as f:
                f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
        if str(txt).find('recent') != -1:
            with open('data_saved/recent_playtime/' + GAMENAME + '.json','w') as f:
                f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
        if str(txt).find('active') != -1:
            with open('data_saved/AU/' + GAMENAME + '.json','w') as f:
                f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])

    
    



# 해당 경우는 찾은거임. / 근데, 다른 경우가 문제. (4개 전부 있는게 아닐때에도, 4개는 전부 출)
# soup.find_all(attrs = {'data-apex-type' : 'line'})[0]

# 존재하는 이름 확인. 다만 한개만 나올때 findall이 되지 않아 문제 발생.
# soup.find_all(attrs = {'type' : 'checkbox'}).find


    
'''
# 기본형
chk_data_name = soup.find_all(attrs = {'type' : 'checkbox'})

for idx, txt in enumerate(chk_data_name):
    if str(txt).find('concurrent') != -1:
        with open('data_saved/CCU/' + GAMENAME + '.json','w') as f:
            f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
    if str(txt).find('popularity') != -1:
        with open('data_saved/popularity/' + GAMENAME + '.json','w') as f:
            f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
    if str(txt).find('recent') != -1:
        with open('data_saved/recent_playtime/' + GAMENAME + '.json','w') as f:
            f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
    if str(txt).find('active') != -1:
        with open('data_saved/AU/' + GAMENAME + '.json','w') as f:
            f.write(str(soup.find_all(attrs = {'data-apex-type' : 'line'})[idx]).split('[')[1].split(']')[0])
'''






#%%

recent_path = glob('data_saved/recent_playtime/*.json')
popularity_path = glob('data_saved/popularity/*.json')
ccu_path = glob('data_saved/CCU/*.json')
au_path = glob('data_saved/AU/*.json')


def read_json_files(path):
    line = pd.read_json(path, lines=True)
    if len(line) == 0:
        return []
    line['x'] = pd.to_datetime(line.x).apply(lambda x : str(x)[:10])
    line.columns = ['DATE', path.replace('.json','').replace('data_saved/','').replace('\\','_')]
    line = line.set_index('DATE')
    return line

# 역할 정리.
# read_json_files(recent_path[1])

# 데이터 프레임 전체 읽고
temp_df = []

for path in recent_path + popularity_path + ccu_path + au_path:
    line = read_json_files(path)
    if len(line) != 0:
        temp_df.append(line)

# 순서대로 조인하고
merge_df = temp_df[0]

for idx in tqdm(range(1, len(temp_df))):
    merge_df = pd.merge(merge_df, temp_df[idx], left_on='DATE', right_on='DATE', how='outer').groupby(level=0).last()


# 완성된 데이터프레임이 mergeDF임. -> full join이므로 마지막 값만 사실로 취급 사용.
merge_df.to_csv('Merge_dataset.csv')
