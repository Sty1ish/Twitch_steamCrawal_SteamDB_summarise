# Twitch_steamCrawal_SteamDB_summarise
트위치 api, 스팀 api 이용 크롤링, steamDB의 raw 데이터 summarise 결과

# 트위치 API
twitch_crawling.py를 통해, 현재 방송중인 전체 페이지를 크롤링합니다. 또한 시간 단위로 계속 저장합니다.

twitch_crawling_finder.py를 통해, 시간단위로 저장된 각 페이지들에 대해 요약치를 구합니다. 이 때 각 게임별로 merge를 실시한 결과를 반환합니다.

steamDB_crawler.py의 초반 부분에서는 steamAPI를 통한 CCU를 확인 가능했습니다.

다만 과거의 CCU정보를 확인 불가능했기에, steamDB에서 일정 주기로 손수 2주간 데이터를 다운후 폴더에 추가하여 줍니다.


Ex. 자료 다운 예제

스팀 DB 사이트 : https://steamdb.info/app/730/charts/

![image](https://github.com/Sty1ish/Twitch_steamCrawal_SteamDB_summarise/assets/89575978/ffc3d45f-934d-474c-8e2d-19d73d536eac)

여기서 다운 가능한 CCU와 LTU데이터를 다운후 

CCU는 코드가 존재하는 폴더의 하위 폴더인 " STEAMDB_DATA /\[CCU] Steam charts/ '각 게임 명'"

LTU는 코드가 존재하는 폴더의 하위 폴더인 " STEAMDB_DATA /\[LTU] Lifetime player count history/ '각 게임 명'" 위치에 누적하여 저장해 줍니다.

그 뒤 merge를 실시하면, 게임 개별에 대한 요약 결과는 "Summarise_Dataset"에 생성되며, LTU와 CCU의 총합 merge결과는 코드 위치에 각각 저장됩니다.


# 그외 참고 가능한 API

[Steam Spy] 

steamspy를 이용하면, 최근 14일 정도의 데이터를 확인 가능함. 추가적인 데이터 또한 확인 가능함.
반면, 크롤링 자체는 유료 계정을 이용중임에도(월 5천원 상당) 따로 접근이 불가능하여 진행하지 못함.

steamspy api
https://github.com/woctezuma/steamspypi

steamspy의 api를 제공하고 있으나, 실시간 데이터를 다운 가능함.
또한 불러온 현재 시점 기준, 당일 데이터만 지급 받기 때문에 시계열 추적이 불가능한 데이터셋임.
https://steamspy.com/api.php 

가격표와, 현재 이용 현황을 다운한다면 이 방법이 가장 빠를 것으로 예상됨.

[togeproductions]
스팀에 올라온 해당 상품의 긍정/부정 평가가 국가별로 어떤 구성을 하고 있는지 확인 가능함.
중장기적 평가에서는 이 시각화가 가장 유용한것으로 판단.
https://www.togeproductions.com/SteamScout/steamAPI.php?appID=323190
