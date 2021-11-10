# -*- coding:utf-8 -*-
import json
import time
import pymysql
from bs4 import BeautifulSoup
import requests

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于福建省人民政府省政府省政府令进行抓取。
"""


class SZFFetcher:
    def __init__(self):
        """
        newest_policy_title用于比较爬取到的最新政策标题如果和已有的一致则视为没有更新放弃爬取，如果非一致则先进行爬取至与已有的一致后再中断爬取
        并且更新最新的政策标题。
        is_first用于标志是否首次读抓取该网站文件比较重要用于判断是否终止抓取。
        """
        with open("record.json", "r") as f:
            self.newest_policy_title = json.loads(f.read())["newest_policy_title"]
            if self.newest_policy_title is None:
                self.is_first = True
            else:
                self.is_first = False
        try:
            self.db = pymysql.connect(
                host='122.112.141.60',
                port=3306,
                user='root',
                password='haodong'
            )
        except Exception as e:
            exit("连接数据库失败")

    def loc_policy(self):
        """
        福建省政府文件库对于请求超出一定的范围返回的json数据中error: true，因此可以根据
        json中的error来判断是否请求结束即爬取内容结束，注意爬取政府政策涉及保密条例，爬取
        速率应降低保证人身安全。
        :return:
        """
        params = {
            "siteId": "8a28cde27b39c3a7017b4e11850d009c",
            "sourceType": "SSP_OPENINFO",
            "apiName": "SSP_OPENINFO",
            "sortFiled": "-docrelTime",
            "themeType": -1,
            "isCollapse": "N",
            "fullKey": "Y",
            "jiGuan": "省政府令",
            "isChange": 1,
            "page": 1,
            "datas": 10
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                          Chrome/94.0.4606.71 Safari/537.36 '
        }  # 请求头部，用于最基本的反爬如果失败则立即停止说明目标文件非公开
        api_url = "http://www.fj.gov.cn/ssp/search/api/apiSearch?"
        preview_data = requests.get(url=api_url, params=params, headers=headers).json()  # 初次请求用于判断是否需要爬取，必要
        if not self.is_first:  # 并非初次抓取政策文件，需要对政策文件是否有必要向后抓取进行判断
            if preview_data["datas"][0]["_doctitle"] == self.newest_policy_title:
                exit(1)  # 说明政策文件还未更新，不具备向后抓取的条件
        while True:
            preview_data = requests.get(url=api_url, params=params, headers=headers).json()  # 正式开始爬取内容
            # 获取当前页面下的政策列表信息
            for policy_info in preview_data["datas"]:  # 对于每一条政策信息存储
                if not self.is_first:  # 对于并非首次抓取政策文件，需要对政策文件判断什么时候停止向后抓取政策
                    if policy_info["_doctitle"] == self.newest_policy_title:
                        pass  # 退出向后抓取需要执行的后续操作
                response = requests.get(url=policy_info["docpuburl"], headers=headers)
                bs = BeautifulSoup(response.content.decode("utf8"), 'lxml')
                content_docker = bs.find("div", {"class": "TRS_Editor"}).find_all("p")  # 找到class为TRS_Editor的p标签
                content_builder = ""  # 构造内容文本字符串
                for part_content in content_docker:
                    if part_content.text is not None:  # 对于所有非空p标签抽取内容文本以组合
                        content_builder += part_content.text
            params["page"] += 1  # 向后一页抓取内容

    def insert_data(self, info: json, content: str):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('''INSERT INTO 
        dpp_policy(title, policy_index, public_unit, issued_number, public_time, complete_time, content, url, source)
        VALUES('%S, %S, %S, %S, %S, %S, %S, %S, %S' % (info["doc_title"]))
        ''')


if __name__ == "__main__":
    tmp = SZFFetcher()
    tmp.loc_policy()
    '''
    db = pymysql.connect(
        host='122.112.141.60',
        port=3306,
        user='root',
        password='haodong'
    )
    cursor = db.cursor()
    cursor.execute("USE knowledge;")
    cursor.execute("SHOW TABLES")
    result = cursor.fetchall()
    print(result)
    exit(1)
    cursor.execute("SHOW DATABASES")
    result = cursor.fetchall()
    print(result)
    '''
    """
    tmp = {
        "newest_policy_title": None
    }
    results = json.dumps(tmp, indent=4)
    f = open("record.json", "w", encoding="utf8")
    f.write(results)
    f.close()
    """

