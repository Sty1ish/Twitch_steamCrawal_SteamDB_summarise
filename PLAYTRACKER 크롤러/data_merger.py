import os
import shutil
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime
from tqdm import tqdm


CODE_PATH = r'python 파일이 존재하는 파일 현재 경로' + '\data_saved'


os.chdir(CODE_PATH)


recent_path = glob('recent_playtime/*.json')
popularity_path = glob('popularity/*.json')
ccu_path = glob('CCU/*.json')
au_path = glob('AU/*.json')


def read_json_files(path):
    line = pd.read_json(path, lines=True)
    line['x'] = pd.to_datetime(line.x).apply(lambda x : str(x)[:10])
    line.columns = ['DATE', path.replace('.json','')]
    line = line.set_index('DATE')
    return line

# 역할 정리.
# read_json_files(recent_path[1])

# 데이터 프레임 전체 읽고
temp_df = []

for path in recent_path + popularity_path + ccu_path + au_path:
    temp_df.append(read_json_files(path))

# 순서대로 조인하고
merge_df = temp_df[0]

for idx in tqdm(range(1, len(temp_df))):
    merge_df = pd.merge(merge_df, temp_df[idx], left_on='DATE', right_on='DATE', how='outer')
    
# 완성된 데이터프레임이 mergeDF임.

merge_df.to_csv('Merge_dataset.csv')
