from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import pandas as pd
from tqdm import tqdm


import warnings
warnings.filterwarnings("ignore")


# 初始化Elasticsearch客户端，启用证书验证，并提供用户名和密码
es = Elasticsearch(
    ["https://127.0.0.1:9200"],
    verify_certs=False,  # 禁用证书验证
    timeout=120,
    basic_auth=("elastic", "123456")  # 提供用户名和密码
)


# 指定索引名称
INDEX_NAME = "mnbvc_law"
### 插入--test


# 删除索引
def delete_index():
    response = es.indices.delete(index=INDEX_NAME)
    print(response)
def insert():
    doc_list = [
        {"_index": INDEX_NAME, "_id": 1, "_source": {"output": "测试数据-----1", "uid": "0ed5b43c-4366-43f4-b638-ff291c5ea60f", "ent0": "氯", "typ0": "物体类_化学物质", "ent1": "日本东京", "typ1": "物体类_化学物质", "ent2": "丁炔", "typ2": "物体类_化学物质", "source": "baike_chinese_new_all", "domain": "待定"}},
        {"_index": INDEX_NAME, "_id": 2, "_source": {"output": "测试数据-----2", "uid": "0eda8f63-c43e-4a48-b757-171a12227d33", "ent0": "日本", "typ0": "世界地区类_国家", "ent1": None, "typ1": None, "ent2": None, "typ2": None, "source": "baike_chinese_new_all", "domain": "待定"}},
    ]

    # 执行批量插入操作
    success, failed = helpers.bulk(es, doc_list)

    # 输出操作结果
    print("成功插入 {} 个文档，失败 {} 个文档".format(success, failed))

### 检索测试 search

def batch_insert():
    root_path=f'/data/ner_classify/{INDEX_NAME}/final/'    
    path_list=os.listdir(root_path)
    total_success=0
    total_failed=0
    for p in tqdm(path_list):
        path=root_path+p
        df=pd.read_parquet(path,engine='pyarrow')
        ##
        # df['domain']='待定'
        df['source']=INDEX_NAME
        # df.rename(columns={'text': 'output'}, inplace=True)

        ##
        dict_list=df.to_dict(orient='records')
        doc_list=[{"_index": INDEX_NAME,"_source":obj} for obj in dict_list]
        success, failed = helpers.bulk(es, doc_list)
        total_success+=success
        total_failed+=len(failed)
    print("成功插入 {} 个文档，失败 {} 个文档".format(total_success, total_failed))

def search(index_list):
    # 准备要检索的关键词
    keyword = "习近平"

    #构建查询语句
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase": {"ent0": keyword}},
                    {"match_phrase": {"ent1": keyword}},
                    {"match_phrase": {"ent2": keyword}}
                ]
            }
        },
        'size':10000
    }

    
    # 执行查询操作
    search_result = es.search(index=index_list, body=query_body)

    # 打印查询结果
    # for hit in search_result['hits']['hits'][:5]:
    #     print(hit['_source'])
    print(len(search_result['hits']['hits']))
    print(search_result['hits']['hits'][-1])
    result_dict={}
    for item in search_result['hits']['hits']:
        item=item['_source']
        key_list=[item['ent0'],item['ent1'],item['ent2']]
        for key in key_list:
            if keyword in key:
                if key not in result_dict:
                    result_dict[key]=[]
                result_dict[key].append(item)
                break
    pass

        
def search_output(keyword):
    index_list=['people_daily_new','zhihu_qa','baike_chinese_new_all']

    query_body = {
        "query": {
            "match": {
                "output": {
                    "query": keyword,
                    "analyzer": "standard",  # 指定分析器，可以根据需要调整
                    "boost": 1.0  # 设置boost参数，根据需要调整
                }
            }
        },
        "size": 10000  # 指定返回的文档数量
    }

    # 执行搜索
    result = es.search(index=index_list, body=query_body)
    print(len(result["hits"]["hits"]))
    return {keyword:[i['_source'] for i in result["hits"]["hits"]]}
          
        
        
# delete_index()
batch_insert()
# search()
# insert()
# index_list=['people_daily_new','zhihu_qa']
# search(index_list)

# a=search_output("EVA 动漫")

