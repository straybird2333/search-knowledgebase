import pandas as pd
import os
import pyarrow.parquet as pq

from tqdm import tqdm
data_set='people_daily_new'
root_path=f'/data/ner_classify/{data_set}/final/'
write_path = f'/data/es/data/{data_set}.jsonl'
path_list=os.listdir(root_path)
path_list=[root_path+i for i in path_list]

dataset = pq.ParquetDataset(path_list)
table = dataset.read()
df = table.to_pandas()    
df['source']=data_set
df['domain']='待定'
# df.rename(columns={'text': 'output'}, inplace=True)
# df=df.loc[:20]

df.to_json(write_path, orient='records', lines=True, force_ascii=False)
print('ok')

