import os
import time
import requests
import schedule

import pandas as pd
from tqdm import tqdm
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser


#%%
CODE_PATH = r'python 파일이 존재하는 파일 현재 경로'
API_KEY = 'Youtube API 키' # 회사계정 api

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_SERVICE_VERSION = 'v3'

os.chdir(CODE_PATH)
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_SERVICE_VERSION, developerKey = API_KEY)

#%%

# 급해서 일단 복붙
# 참조 링크.
# https://velog.io/@ssongji/%EC%9C%A0%ED%8A%9C%EB%B8%8C-%EB%8D%B0%EC%9D%B4%ED%84%B0-%ED%81%AC%EB%A1%A4%EB%A7%81-%EB%B0%8F-%EC%8B%9C%EA%B0%81%ED%99%94-%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8-2.-YOUTUBE-API%EB%A1%9C-%ED%82%A4%EC%9B%8C%EB%93%9C-%EA%B2%80%EC%83%89-%EB%B0%8F-%ED%8A%B9%EC%A0%95-%EC%9C%A0%ED%8A%9C%EB%B2%84-%EB%8D%B0%EC%9D%B4%ED%84%B0-%EC%B6%94%EC%B6%9C
# https://developers.google.com/youtube/v3/code_samples/python?hl=ko#search_by_keyword


# 토픽 ID Freebase topic ID를 참조하고 이 태그는 수천만개 존재.
# https://gist.github.com/stpe/2951130dfc8f1d0d1a2ad736bef3b703 이런 형태로 존재한다 하는데...
# https://support.dittomusic.com/en/articles/6701114-how-do-i-find-my-youtube-topic-channel-id 얘 확인하면.
# https://www.youtube.com/channel/UCKSRXvvdLRZrSOGw0M_b15w # 스페이스 기어즈 이후에... 표시되는데. channel 이후 아이디임.


# 매개변수는 여기서 사용. https://developers.google.com/youtube/v3/docs/search/list?hl=ko
# publishedAfter를 활용해야 추세 파악에 효과적이겠지  값은 RFC 3339 형식
# publishedBefore를 활용하고 (이전에 만들어진 영상.)

# 토픽 한정 검색을 한다면
search_something = youtube.search().list(
    topicId = 'UCKSRXvvdLRZrSOGw0M_b15w',
    q = 'space gears',   # 검색어
    part = 'snippet',    # 
    order = 'relevance', # 조회 기준
    maxResults = 10,
).execute()


search_something = youtube.search().list(
    q = 'space gears',       # 검색어
    part = 'snippet',    # 
    order = 'relevance', # 조회 기준
    maxResults = 50, # 최대치가 50 반환.
).execute()

'''

date – 리소스를 만든 날짜를 기준으로 최근 항목부터 시간 순서대로 리소스를 정렬합니다.
rating – 높은 평가부터 낮은 평가순으로 리소스를 정렬합니다.
relevance – 검색 쿼리에 대한 관련성을 기준으로 리소스를 정렬합니다. 이 매개변수의 기본값입니다.
title – 제목에 따라 문자순으로 리소스를 정렬합니다.
videoCount – 업로드한 동영상 수에 따라 채널을 내림차순으로 정렬합니다.
viewCount – 리소스를 조회수가 높은 항목부터 정렬합니다.

# 즉 order기준을 date로 하고 최신화 유지하면 가능.
'''

# 마찬가지로 page토큰이 존재함. 트위치와 동일.
next_page = search_something['nextPageToken']

search_something = youtube.search().list(
    q = 'space gears',       # 검색어
    part = 'snippet',    # 
    order = 'relevance', # 조회 기준
    maxResults = 50, # 최대치가 50 반환..
    pageToken = next_page
).execute()


youtube.search()


#%%
# 여기서 부터 본론.

def channel_stat(channel_id):
    channel_response = youtube.channels().list(part="statistics", id = channel_id).execute()
    return channel_response


def set_video(next_page):  
 	#먼저 search 함수를 통해 해당 channelId의 조회 수(viewCount)가 높은 영상을 파라미터로 받아 준 maxCount만큼 조회한다.
   search_response = youtube.search().list(
       topicId = 'UCKSRXvvdLRZrSOGw0M_b15w', # 스페이스 기어스 토픽 ID.
       q = 'space gears',       # 검색어
       order = 'date',          # 최신 기준 검색 실시.
       # order = 'viewCount',
       part = 'snippet',
       type = 'video',           # 비디오 한정 검색. 
       maxResults = 50,
       #pageToken = next_page,
   ).execute()
   
   # 다음 페이지 번호 저장.
   next_page_token = search_response['nextPageToken']
   
   temp_line = [i['snippet']['channelId'] for i in search_response['items']]
   #채널 정보를 가지고 오면 구독자를 가지고 올 수 있다. search()가 아닌 channels()로 조회해서 채널 정보를 조회한다.
   channel_response = [channel_stat(i) for i in temp_line]
   
   video_ids = []
   for i in range(0, len(search_response['items'])):
       video_ids.append((search_response['items'][i]['id']['videoId'])) #videoId의 리스트를 만들어 둔다 (videoId로 조회할 수 있게)
 
 	#추출할 정보의 list들과 그 모든 정보를 key-value로 저장할 딕셔너리 변수를 하나 생성한다.
   channel_video_id = []
   channel_video_title = []
   channel_rating_view = []
   channel_rating_comments = []
   channel_rating_good = []
   channel_published_date = []
   channel_subscriber_count = []
   channel_thumbnails_url = []
   data_dicts = {}
   
   # 영상이름, 조회수 , 좋아요수 등 정보 등 추출
   for k in range(0, len(search_response['items'])):
       video_ids_lists = youtube.videos().list(
           part='snippet, statistics',
           id = video_ids[k],
       ).execute()
       
       #print(video_ids_lists)
   
       str_video_id = video_ids_lists['items'][0]['id']
       str_thumbnails_url = str(video_ids_lists['items'][0]['snippet']['thumbnails']['high'].get('url'))
       str_video_title = video_ids_lists['items'][0]['snippet'].get('title')
       str_view_count = video_ids_lists['items'][0]['statistics'].get('viewCount')
       if str_view_count is None:
           str_view_count = "0"
       str_like_count = video_ids_lists['items'][0]['statistics'].get('likeCount')
       if str_like_count is None:
           str_like_count = "0"
       str_published_date = str(video_ids_lists['items'][0]['snippet'].get('publishedAt'))
       
       
       # 비디오 ID 
       channel_video_id.append(str_video_id)
       # 비디오 제목 
       channel_video_title.append(str_video_title)
       # 조회수 
       channel_rating_view.append(str_view_count)
       # 좋아요 
       channel_rating_good.append(str_like_count)
       # 게시일 
       channel_published_date.append(str_published_date)
       
       channel_thumbnails_url.append(str_thumbnails_url)
       
   # 구독자 수
   for follower_cnt in channel_response:
       str_subscriber_count = follower_cnt['items'][0]['statistics']['subscriberCount']
       
       if str_subscriber_count is None:
           str_subscriber_count = "0"
       
       # 구독자 수 
       channel_subscriber_count.append(str_subscriber_count)


   data_dicts['id'] = channel_video_id
   data_dicts['title'] = channel_video_title
   data_dicts['viewCount'] = channel_rating_view
   data_dicts['likeCount'] = channel_rating_good
   data_dicts['publishedDate'] = channel_published_date
   data_dicts['subsciberCount'] = channel_subscriber_count 
   data_dicts['thumbnail'] = channel_thumbnails_url
   
   return data_dicts, next_page_token

# 기본 사용법.
# youtube_videos, next_pg = set_video(None)


def Search_Result(page_num):
    df = []
    next_pg = None

    for _ in tqdm(range(2)):
        youtube_videos, next_pg = set_video(None)
        df.append(pd.DataFrame(youtube_videos))
        
    df = pd.concat(df, axis = 0)
    df['id'] = df['id'].apply(lambda x : 'https://www.youtube.com/watch?v=' + str(x))
    
    return df
    


def schedule_do():
    df = Search_Result(2)
    df.to_csv('YouTube_Dataset[recent100]/'+datetime.now().strftime("%y-%m-%d %H%M%S")+'(hhmmss KST).csv', index = False, encoding = 'UTF-8')


# 1분에 한번씩 실행
schedule.every(4).hours.do(schedule_do)

# 크롤러 항상 켜져있도록.
while True:
    schedule.run_pending()
    time.sleep(1)
