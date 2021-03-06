# -*- coding:utf-8 -*-
import json
import time
import lxml
import pymysql
from bs4 import BeautifulSoup
import requests
from lxml import etree, html

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于福建省人民政府省政府进行抓取。
"""


class SZFFetcher:
    def __init__(self):
        """
        newest_policy_title用于比较爬取到的最新政策标题如果和已有的一致则视为没有更新放弃爬取，如果非一致则先进行爬取至与已有的一致后再中断爬取
        并且更新最新的政策标题。
        is_first用于标志是否首次读抓取该网站文件比较重要用于判断是否终止抓取。
        """
        with open("record.json", "r", encoding="utf8") as f:
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
            """
            # 警告: 本部分代码仅适用于测试时删除无用数据
            cursor = self.db.cursor()
            cursor.execute("USE knowledge")
            cursor.execute("DELETE FROM dpp_policy")
            self.db.commit()
            exit(1)
            """
        except pymysql.err.OperationalError as e:
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

        first_title_flag = True  # 用于判断是否需要将获取到的标题记录为最新政策标题
        first_title_recorder = ""  # 用于记录最新政策标题

        while True:
            preview_data = requests.get(url=api_url, params=params, headers=headers).json()  # 正式开始爬取内容
            if preview_data["error"] or preview_data["datas"] == []:  # 初次爬取需要判断什么时候停止向后抓取政策
                self.renew_op(first_title_recorder)
                exit(1)  # 退出当前进程
            print("正在抓取第" + str(params["page"]) + "页政策列表")  # 打印爬取页数便于Debugger(
            # 获取当前页面下的政策列表信息
            for policy_info in preview_data["datas"]:  # 对于每一条政策信息存储
                if policy_info["doctype"] == 40:
                    continue  # 对于非超文本的文档类型丢弃例如pdf，页面可能会直接加载pdf而非html这部分无法处理需要抛弃

                if first_title_flag:
                    first_title_recorder = policy_info["_doctitle"]  # 更新最新政策标题
                    first_title_flag = False  # 并将标识符置为False后续无需更新

                if not self.is_first:  # 对于并非首次抓取政策文件，需要对政策文件判断什么时候停止向后抓取政策
                    if policy_info["_doctitle"] == self.newest_policy_title:
                        self.renew_op(first_title_recorder)  # 退出向后抓取需要执行的后续操作
                        exit(1)  # 退出当前进程

                response = requests.get(url=policy_info["docpuburl"], headers=headers)
                bs = BeautifulSoup(response.text.encode(response.encoding), 'lxml')

                content_docker = bs.find_all({"h1", "h2", "h3", "h4", "h5", "h6", "p"})  # 找到所有的p和h标签对内容进行过滤
                content_builder = ""  # 构造内容文本字符串
                dinner_begin = False  # 表示”正餐“是否开始，即是否正文内容仅能支持过滤掉部分内容
                for part_content in content_docker:
                    if policy_info["_doctitle"] in part_content.text:
                        dinner_begin = True  # 正文即将开始
                    if dinner_begin:
                        if part_content.text is not None:  # 对于所有非空p标签抽取内容文本以组合
                            content_builder += part_content.text
                if content_builder != "":  # 对于爬取到的非空才执行插入数据库操作
                    self.insert_data(policy_info, content_builder)
            time.sleep(1)
            params["page"] += 1  # 向后一页抓取内容

    def insert_data(self, info: json, content: str):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        if "idxid" not in info or "fileno" not in info:  # 如果文件不存在政策索引甚至发文字号不视为政策文件
            return
        print("正在抓取:    " + info["docpuburl"])
        info_tuple = (info["_doctitle"], info["idxid"], info["puborg"], info["fileno"], info["pubdate"],
                      info["docreltime"], content, info["docpuburl"], info["puborg"], "福建")
        content.encode("utf-8")
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('USE knowledge')
        try:
            cursor.execute('''INSERT INTO 
            dpp_policy(title, policy_index, public_unit, issued_number, public_time, complete_time, content, url, 
                source, province)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', info_tuple)  # 向远端数据库插入数据
        except pymysql.err.DataError:
            print("编码存在一定转换问题放弃抓取")
            cursor.close()
            return
        self.db.commit()
        cursor.close()  # 游标对象关闭

    def renew_op(self, newest_policy_title):
        """
        爬取结束后更新record.json将最新政策标题插入进去，原先的loc_file功能实在过于臃肿不应该再添加善后工作
        :param newest_policy_title: 最新的政策标题
        :return:
        """
        tmp_json = {
            "newest_policy_title": newest_policy_title
        }
        results = json.dumps(tmp_json, indent=4, ensure_ascii=False)
        f = open("record.json", "w", encoding="utf8")
        f.write(results)
        f.close()


if __name__ == "__main__":
    """
    response = requests.get("http://zrzyt.fujian.gov.cn/zwgk/zfxxgkzl/zfxxgkml/jsxmydsp/202111/t20211105_5768520.htm")
    bs = BeautifulSoup(response.text.encode(response.encoding), 'lxml')

    content_docker = bs.find_all({"h1", "h2", "h3", "h4", "h5", "h6", "p"})  # 找到所有的p标签对内容进行过滤
    content_builder = ""  # 构造内容文本字符串
    begin = False
    for part_content in content_docker:
        if part_content.text == "福建省人民政府关于安溪县2021年度第六批次农用地转用和土地征收的批复":
            begin = True
        if begin:
            if part_content.text is not None:  # 对于所有非空p标签抽取内容文本以组合
                content_builder += part_content.text
    with open("results.txt", "w", encoding="utf8") as f:
        f.write(content_builder)
    exit(1)
    """
    tmp = SZFFetcher()
    tmp.loc_policy()
