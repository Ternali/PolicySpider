# -*- coding:utf-8 -*-
import json
import time
import bs4
import requests

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
由于福建省未将各市政策纳入故采取首先省政府然后根据车牌号决定爬取顺序
"""


class FJProvinceFetcher:
    def __init__(self):
        """
        is_first用于判断是否初次定位文件，从判断是否初次文档record.json中获取
        policy_num用于统计福建省省政府政策文件数量
        policy_title_store用于记录获得政策文件标题，注意为了筛选重复采用Set后转列表存储
        """
        self.is_first = True
        self.policy_num = 0
        self.policy_title_store = set()
        self.policy_url_store = set()

    def load_data(self):
        """
        通过特定的record.txt内容，第一行为是否初次爬取政策文件，第二行为
        当前人民政府政策数量，第三行为政策标题集合，第四行为政策对应的url，
        :return:
        """
        with open("record.json", "r", encoding="utf8") as f:
            tmp_json = json.loads(f.read())
            self.is_first = tmp_json["is_first"]
            self.policy_num = tmp_json["policy_num"]
            self.policy_title_store = tmp_json["policy_title_Store"]
            self.policy_url_store = tmp_json["policy_url_store"]

    def make_strategy(self):
        """
        用于判断是否初次定位政策，并执行后续的操作
        :return:
        """
        if self.is_first:
            self.first_loc_policy()
        else:
            self.first_loc_policy()
            # loc_policy()测试暂时注释

    def first_loc_policy(self):
        """
        福建省政府文件库对于请求超出一定的范围返回的json数据中error: true，因此可以根据
        json中的error来判断是否请求结束即爬取内容结束，注意爬取政府政策涉及保密条例，爬取
        速率应降低保证人身安全。
        :return:
        """
        tmp = requests.get(url="http://sft.fujian.gov.cn/zwgk/zfxxgkzl/zfxxgkml/fggzhgf/gfxwj/202111/t20211108_5769185.htm")
        content = bs4.BeautifulSoup(tmp.content.decode('utf8'), 'lxml')
        p_elements = content.find_all('div', attrs={'class': 'w_1300'})
        for p_element in p_elements:
            if p_element.text is None:
                continue
            print(p_element.text)
        exit(1)
        params = {
            "siteId": "8a28cde27b39c3a7017b4e11850d009c",
            "sourceType": "SSP_OPENINFO",
            "apiName": "SSP_OPENINFO",
            "sortFiled": "-docrelTime",
            "themeType": -1,
            "isCollapse": "N",
            "fullKey": "Y",
            "isChange": 1,
            "page": 1,
            "datas": 10
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                  Chrome/94.0.4606.71 Safari/537.36 '
        }  # 请求头部
        api_url = "http://www.fj.gov.cn/ssp/search/api/apiSearch?"
        preview_data = requests.get(url=api_url, params=params, headers=headers).json()
        """
        tmp_json = json.dumps(preview_data, indent=4, ensure_ascii=False)
        with open("FJProvinceData.json", "w", encoding="utf8") as f:
            f.write(tmp_json)
        print(preview_data)
        """

    def loc_policy(self):
        pass


if __name__ == "__main__":
    test = FJProvinceFetcher()
    test.first_loc_policy()
    """
    tmp = {
        "is_first": True,
        "policy_num": 0,
        "policy_title_store": [],
        "policy_title_url": []
    }
    results = json.dumps(tmp, indent=4)
    f = open("record.json", "w", encoding="utf8")
    f.write(results)
    f.close()
    """
