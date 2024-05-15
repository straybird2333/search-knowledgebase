from flask import Flask, render_template, request, send_file
import pandas as pd
from io import BytesIO
import zipfile

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import warnings
warnings.filterwarnings("ignore")
app = Flask(__name__)

es = Elasticsearch(
    ["https://127.0.0.1:9200"],
    verify_certs=False,  # 禁用证书验证
    timeout=120,
    basic_auth=("elastic", "123456")  # 提供用户名和密码
)
index_list=['people_daily_new','baike_chinese_new_all','gov_safety','zhihu_qa']


def search_query_str(query_str,index_list):
    query_body = {
        "query": {
            "match": {
                "output": {
                    "query": query_str,
                    "analyzer": "standard",  # 指定分析器，可以根据需要调整
                    "boost": 1.0  # 设置boost参数，根据需要调整
                }
            }
        },
        "size": 10000  # 指定返回的文档数量
    }

    # 执行搜索
    result = es.search(index=index_list, body=query_body)
    return [i['_source'] for i in result["hits"]["hits"]]

def search_key(key,index_list):
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase": {"ent0": key}},
                    {"match_phrase": {"ent1": key}},
                    {"match_phrase": {"ent2": key}}
                ]
            }
        },
        'size':10000
    }
    result = es.search(index=index_list, body=query_body)
    return [i['_source'] for i in result["hits"]["hits"]]

def search_domain(domain,index_list):
    
    query_body = {
    "query": {
        "term": {
            "domain.keyword": {
                "value": domain  # 指定精确匹配的关键字
            }
        }
    },
    "size": 10000  # 指定返回的文档数量
}
    result = es.search(index=index_list, body=query_body)
    return [i['_source'] for i in result["hits"]["hits"]]

def search_type(ent_type,index_list):
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase": {"typ0": ent_type}},
                    {"match_phrase": {"typ1": ent_type}},
                    {"match_phrase": {"typ2": ent_type}}
                ]
            }
        },
        'size':10000
    }
    result = es.search(index=index_list, body=query_body)
    return [i['_source'] for i in result["hits"]["hits"]]

def filter_key(key,result):
    new_result=[]
    for item in result:
        key_list=[item['ent0'],item['ent1'],item['ent2']]
        for item_key in key_list:
            if item_key!=None and key in item_key:
                new_result.append(item)
                break
    
    return new_result

def filter_domain(domain,result):
    new_result=[]
    for item in result:
        if domain==item['domain']:
            new_result.append(item)
    return new_result

def filter_type(ent_type,result):
    new_result=[]
    for item in result:
        key_list=[item['typ0'],item['typ1'],item['typ2']]
        for item_key in key_list:
            if ent_type!=None and ent_type in item_key:
                new_result.append(item)
                break
    return new_result



@app.route('/')
def index():
    return render_template('index.html')
@app.route('/query', methods = ['GET'])
def query():
    
    
    
    # result={}
    key = request.args.get('entity')
    domain=request.args.get('domain')
    source=request.args.get('source')
    query_str=request.args.get('query_str')
    # ent_type=request.args.get('type')
    ent_type=""
    
    
    
    
    
    index_list=[]
    if source!="" and (key!=None or domain!=None or query_str!=None):
        index_list.append(source)
        
        
    if len(index_list)==0:
        index_list=['people_daily_new','baike_chinese_new_all','gov_safety','zhihu_qa']
    flag=0
    if query_str!="":
        result=search_query_str(query_str,index_list)   
        flag=1 
    if key !="":
        if flag:
            result=filter_key(key,result)
        else:
            result=search_key(key,index_list)
            flag=1
    if domain!="":
        if flag:
            result=filter_domain(domain,result)
        else:
            result=search_domain(domain,index_list)
            flag=1
    if ent_type!="":
        if flag:
            result=filter_type(ent_type,result)
        else:
            result=search_type(ent_type,index_list)
            flag=1
            
    
    file_name=f'{key}-{query_str}-{domain}-{source}-{ent_type}'
    result={file_name:result}
    pass

    zip_data=BytesIO()
    with zipfile.ZipFile(zip_data,'w') as zip_file:
        for key_name in result.keys():
            df=result[key_name]
            df = pd.DataFrame(df)
            csv_data=BytesIO()
            df.to_csv(csv_data,index=False)
            csv_data.seek(0)
            zip_file.writestr(key_name+'.csv',csv_data.getvalue())
            
    zip_data.seek(0)
    return send_file(zip_data,mimetype='application/zip',as_attachment=True,download_name=f'{key}.zip')
app.run(host='0.0.0.0', port=12345, debug=True)