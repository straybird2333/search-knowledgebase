alias ecurl='curl --cacert /etc/elasticsearch/certs/http_ca.crt -u "elastic:123456"'
ecurl -X GET  'http://localhost:9200/_cat/indices?v' # 好像有点问题
ecurl https://127.0.0.1:9200
ecurl -X PUT https://localhost:9200/test_1
ecurl --cacert /etc/elasticsearch/certs/http_ca.crt -u "elastic:123456" -X GET "https://localhost:9200/_cat/indices?v"
ecurl -X PUT -H "content-type:application/json;charset=utf-8" https://localhost:9200/test_1/_settings -d "{\"number_of_replicas\": 0}"


ecurl -X POST -H "content-type:application/json;charset=utf-8" https://localhost:9200/test_1/_mapping -d '{
  "properties": {
    "ent0": {
      "type": "text"
    },
    "ent1": {
      "type": "text"
    },
    "ent2": {
      "type": "text"
    },
    "domain": {
      "type": "keyword"
    },
    "output": {
      "type": "text"
    },
    "typ0": {
      "type": "text"
    },
    "typ1": {
      "type": "text"
    },
    "typ2": {
      "type": "text"
    }
  }
}'
ecurl -X GET https://localhost:9200/test_1/_mapping



# 导入索引
ecurl -X POST -H "Content-Type: application/json" --data-binary "@/data/es/data/test.jsonl" "https://localhost:9200/test_1/_bulk"
