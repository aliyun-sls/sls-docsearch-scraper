import time
from typing import Dict, Any
from Tea.exceptions import TeaException
from Tea.request import TeaRequest
from alibabacloud_tea_util import models as util_models
from scraper.src.BaseRequest import Config, Client
import json
import os

# 配置统一的请求入口 注意：host需要去掉http://
endpoint = os.environ.get('API_ENDPOINT', '')
access_key_id = os.environ.get('API_KEY_ID', '')
access_key_secret = os.environ.get('API_KEY_SECRET', '')
app_name = os.environ.get('API_APP_NAME', '')
table_name = os.environ.get('API_APP_TABLE_NAME', '')

class OpenSearchHelper:
    """OpenSearchHelper"""

    def __init__(self):
        self.save_objects = []
        print('')

    def docBulk(self, app_name: str, table_name: str, doc_content: list) -> Dict[str, Any]:
        try:
            response = self.Clients._request(method="POST",
                                             pathname=f'/v3/openapi/apps/{app_name}/{table_name}/actions/bulk', query={}, headers=self.header,
                                             body=doc_content, runtime=self.runtime)
            return response
        except Exception as e:
            print(e)

    def add_records(self, records, url, from_sitemap):
        """Add new records to the temporary index"""
        record_count = len(records)

        for i in range(0, record_count):
            self.save_objects.append(records[i])

        color = "96" if from_sitemap else "94"

        print(
            '\033[{}m> DocSearch: \033[0m{}\033[93m {} records\033[0m)'.format(
                color, url, record_count))

    # def add_synonyms(self, synonyms):
    #     synonyms_list = []
    #     for _, value in list(synonyms.items()):
    #         synonyms_list.append(value)

    #     self.algolia_index_tmp.save_synonyms(synonyms_list)
    #     print(
    #         '\033[94m> DocSearch: \033[0m Synonyms (\033[93m{} synonyms\033[0m)'.format(
    #             len(synonyms_list)))

    def commit_tmp_index(self):
        # 支持 protocol 配置 HTTPS/HTTP
        endpoint_protocol = "HTTP"
        # 支持 type 配置 sts/access_key 鉴权. 其中 type 默认为 access_key 鉴权. 使用 sts 可配置 RAM-STS 鉴权.
        # 备选参数为:  sts 或者 access_key
        # auth_type = "sts"
        # # 如果使用 RAM-STS 鉴权, 请配置 security_token, 可使用 阿里云 AssumeRole 获取 相关 STS 鉴权结构.
        # security_token = "<security_token>"
        # 配置请求使用的通用信息.
        # 注意：security_token和type参数，如果不是子账号需要省略
        config = Config(endpoint=endpoint, access_key_id=access_key_id,
                        access_key_secret=access_key_secret, protocol=endpoint_protocol)

        self.Clients = Client(config)
        self.runtime = util_models.RuntimeOptions(
            connect_timeout=10000,
            read_timeout=10000,
            autoretry=False,
            ignore_ssl=False,
            max_idle_conns=50,
            max_attempts=3
        )
        self.header = {}

        # ---------------  文档推送 ---------------

        documents = []
        record_count = len(self.save_objects)

        for i in range(0, record_count):
            document = {
                "cmd": "ADD",
                "fields": {
                    "id": i,
                    "content_s": self.save_objects[i]['content'],
                    "content": self.save_objects[i]['content'],
                    "lvl0": self.save_objects[i]['hierarchy_radio']['lvl0'],
                    "lvl0_s": self.save_objects[i]['hierarchy_radio']['lvl0'],
                    "lvl1": self.save_objects[i]['hierarchy_radio']['lvl1'],
                    "lvl1_s": self.save_objects[i]['hierarchy_radio']['lvl1'],
                    "lvl2": self.save_objects[i]['hierarchy_radio']['lvl2'],
                    "lvl2_s": self.save_objects[i]['hierarchy_radio']['lvl2'],
                    "lvl3": self.save_objects[i]['hierarchy_radio']['lvl3'],
                    "lvl3_s": self.save_objects[i]['hierarchy_radio']['lvl3'],
                    "lvl4": self.save_objects[i]['hierarchy_radio']['lvl4'],
                    "lvl4_s": self.save_objects[i]['hierarchy_radio']['lvl4'],
                    "lvl5": self.save_objects[i]['hierarchy_radio']['lvl5'],
                    "lvl5_s": self.save_objects[i]['hierarchy_radio']['lvl5'],
                    # "lv6": self.save_objects[i]['hierarchy_radio']['lvl6'],
                    # "lv6_s": self.save_objects[i]['hierarchy_radio']['lvl6'],
                    "url": self.save_objects[i]['url'],
                    "url_widthout_variables": self.save_objects[i]['url_without_variables'],
                    "url_widthout_anchor": self.save_objects[i]['url_without_anchor'],
                    "page_rank": self.save_objects[i]['weight']['page_rank'],
                    "position": self.save_objects[i]['weight']['position'],
                    "level": self.save_objects[i]['weight']['level'],
                    "anchor": json.dumps({
                        "anchor":  self.save_objects[i]['anchor'],
                        "type": self.save_objects[i]['type'],
                        "lvl0": self.save_objects[i]['hierarchy']['lvl0'],
                        "lvl1": self.save_objects[i]['hierarchy']['lvl1'],
                        "lvl2": self.save_objects[i]['hierarchy']['lvl2'],
                        "lvl3": self.save_objects[i]['hierarchy']['lvl3'],
                        "lvl4": self.save_objects[i]['hierarchy']['lvl4'],
                        "lvl5": self.save_objects[i]['hierarchy']['lvl5'],
                        "lvl6": self.save_objects[i]['hierarchy']['lvl6'],
                        "objectID": self.save_objects[i]['objectID'],
                    }, ensure_ascii=False)
                }
            }
            documents.append(document)

        document_count = len(documents)

        for i in range(0, document_count, 50):
            res5 = self.docBulk(app_name=app_name,
                                table_name=table_name, doc_content=documents[i:i + 50])
            print(res5)

        # timestamp 信息 用以增加对 文档操作的保序能力. 系统会用该时间戳来作为同一主键文档更新顺序的判断标准.
        # 在没有该timestamp项时，默认以文档发送到OpenSearch的时间作为文档更新时间进行操作。
        # document1 = {"cmd": "ADD", "timestamp": int(time.time() * 1000), "fields": {"id": "1", "title": "opensearch"}}
        # document2 = {"cmd": "ADD", "fields": {
        #     "id": 2, "content": "avg函数", "content_s": "avg函数"}}
        # # 删除记录
        # # deletedoc={"cmd": "DELETE", "fields": {"id": 2}}
        # # 更新记录
        # # updatedoc={"cmd": "UPDATE", "fields": {"id": 2, "describe": "6666","title": "开放搜索"}}
        # documents = [document2]
        # res5 = self.docBulk(app_name=app_name,
        #                     table_name=table_name, doc_content=documents)
        # print(res5)
