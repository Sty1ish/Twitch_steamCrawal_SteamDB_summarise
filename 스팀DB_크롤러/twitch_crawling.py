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

twitch_game_id = {'Space Gears' : '185794439',
                  'VALORANT' : '516575',
                  'AGE OF EMPIRES III' : '7830',
                  'AGE OF EMPIRES II' : '13389',
                  'AGE OF EMPIRES' : '9623',
                  'CRUSADER KINGS III' : '514888',
                  'Dota2' : '29595',
                  'LEAGUE OF LEGENDS' : '21779',
                  'Europa Universalis IV' : '67584',
                  'Hearts of Iron IV' : '459327',
                  'Iron Harvest' : '498423',
                  "SID MEIER CIVILIZATION IV" : '15084',
                  'SID MEIER CIVILIZATION V' : '27103',
                  'SMITE' : '32507',
                  'Starship Troopers' : '10297',
                  'Stellaris' : '491572',
                  'SUPREME COMMANDER FORGED ALLIANCE' : '16553',
                  'TOTAL WAR: WARHAMMER III' : '1913410799'
                  }


#%%

# 아하.... 이해 완료.
def all_stream(arg):
    twitch_stream = []
    
    # 첫번째는 이렇게 진행하고
    file = requests.get('https://api.twitch.tv/helix/streams?first=100', headers = arg)
    twitch_stream.append(file.json())
    page_cnt = 0
    
    # 두번째 페이지 부터.
    while True:
        # 다음 페이지 ID    
        PAGINATION = file.json()['pagination']
        
        # 다음 페이지가 존재하지 않을때 종료.
        if len(PAGINATION) == 0: 
            print(f'크롤링 완료 총 : {page_cnt} 페이지')
            break
        
        # 페이지가 존재할때는 커서 객체가 존재.
        PAGINATION = PAGINATION['cursor']
        
        # 속도에 따라서 똑같은 페이지 중복 로깅도 됨.
        file = requests.get(f'https://api.twitch.tv/helix/streams?first=100&after={PAGINATION}', headers = arg)
        twitch_stream.append(file.json())
        
        page_cnt += 1
        if page_cnt % 10 == 0:
            print(f'{page_cnt} 번째 페이지 크롤링중.')
    
    twitch_stream_df = []
    
    print('데이터 병합 시작')
    for line in tqdm(twitch_stream):
        if len(line['data']) == 0:
            continue
        twitch_stream_df.append(pd.DataFrame(line['data']))
    twitch_stream_df = pd.concat(twitch_stream_df, axis = 0)
    
    print('데이터 병합 종료')
    twitch_stream_df = twitch_stream_df.drop_duplicates(['id']).reset_index(drop = True)
    return twitch_stream_df



#%%


def do_crawl():
    CODE_PATH = r'python 파일이 존재하는 파일 현재 경로'

    # normal setting
    os.chdir(CODE_PATH)

    # 트위치 API 확인중.

    CLIENT_ID = '트위치 API 클라이언트 ID'
    CLIENT_SECRET = '트위치 API 클라이언트 시크릿 ID'

    # 앱 토큰 받아오기.
    req = requests.post(f'https://id.twitch.tv/oauth2/token?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type=client_credentials')
    print(f'Reqeust 토큰 : {req.text}')

    # 앱 토큰
    APP_TOKEN = eval(req.text)['access_token']
    print(f'사용 토큰명 : {APP_TOKEN}, 위와 동일한지 확인바람.')
    
    # 스트림 확인.
    arg = {'Client-Id': CLIENT_ID, 'Authorization': f'Bearer {APP_TOKEN}'}
    
    # 코드 실행.
    stream_log = all_stream(arg = arg)

    # 결과 폴더 제작.
    if not os.path.exists('Twitch_Dataset'):
        os.mkdir('Twitch_Dataset')

    # 결과 저장.
    stream_log.to_csv('Twitch_Dataset/'+datetime.now().strftime("%y-%m-%d %H%M%S")+'(hhmmss KST).csv', index = False, encoding = 'UTF-8')


# 15분에 한번씩 실행
schedule.every(15).minutes.do(do_crawl)

# 크롤러 항상 켜져있도록.
while True:
    schedule.run_pending()
    time.sleep(1)
    
    
    
    
    
    
    
 
#%%

'''
# 특정 통계치 (누적) 확인
# 채널 관리자(해당 게임의)가 허락한 계정의 경우, API를 사용가능, 그게 아니라면
# 매 시간마다, 해당 게임을 스트리밍 중인 스트리머를 찾아서 계산해야함.
# 근거 : https://discuss.dev.twitch.tv/t/how-to-scraping-all-twitch-game-information/34250
params = {'game_id' : '33214'}
file = requests.get('https://api.twitch.tv/helix/analytics/games', headers = arg, params = params)
file.json()
'''

#%%
# 이상하게 중복이 생긴다;; -> 코딩상 아무런 문제가 없는데 필터링이 되지 않는다;
# 중복값 제거 과정 필요.
   
    
    
