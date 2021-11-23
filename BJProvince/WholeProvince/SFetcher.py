# -*- coding:utf-8 -*-
import json
from json import JSONDecodeError
import time
import pymysql
import requests
from requests import adapters
from requests.packages import urllib3
import trafilatura
import pandas

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于北京整个市进行抓取。
"""


class SZFFetcher:
    def __init__(self):
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
            # 建议改为cursor.execute("DELETE FROM dpp_policy WHERE province == "浙江")
            self.db.commit()
            exit(1)
            """
        except pymysql.err.OperationalError as e:
            exit("连接数据库失败")

    def loc_policy(self):
        """
        注意爬取政府政策涉及保密条例，爬取
        速率应降低保证人身安全。
        :return:
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                          Chrome/94.0.4606.71 Safari/537.36 ',
        }  # 请求头部，用于最基本的反爬如果失败则立即停止说明目标文件非公开

        requests.packages.urllib3.disable_warnings()  # 部分网站SSL问题，忽略
        key_params = ["通知", "决议", "决定", "命令", "公报", "公告", "通告", "意见",
                      "通报", "报告", "请示", "批复", "议案", "函", "纪要"]
        # title, policy_index, public_unit, issued_number,
        # public_time, complete_time, content, url, source, province

        for k in range(0, len(key_params)):
            info = pandas.read_excel(key_params[k] + ".xlsx")
            title_list = []  # 标题对应的列表
            policy_index_list = []  # 索引号对应的列表
            public_unit_list = []  # 发文机关对应的列表
            issued_number_list = []  # 发文字号对应的列表
            public_time_list = []  # 发文时间对应的列表
            complete_time_list = []  # 成文时间对应的列表
            url_list = []  # 链接对应的列表
            if "标题" in info.columns:
                title_list = info.loc[:, "标题"].tolist()
            if "索引号" in info.columns:
                policy_index_list = info.loc[:, "索引号"].tolist()
            if "发文机关" in info.columns:
                public_unit_list = info.loc[:, "发文机关"].tolist()  # 与source等价
            if "发文字号" in info.columns:
                issued_number_list = info.loc[:, "发文字号"].tolist()
            if "发文时间" in info.columns:
                public_time_list = info.loc[:, "发文时间"].tolist()
            if "成文时间" in info.columns:
                complete_time_list = info.loc[:, "成文时间"].tolist()
            if "链接" in info.columns:
                url_list = info.loc[:, "链接"].tolist()

            for i in range(0, len(url_list)):
                title = "无标题"
                policy_index = "无索引号"
                public_unit = "无发文机关"
                issued_number = "无发文字号"
                public_time = "1949-10-01"
                complete_time = "1949-10-01"
                source = "无发文机关"
                province = "北京"
                try:  # 开始爬取政策列表
                    requests.adapters.DEFAULT_RETRIES = 20  # 设置重连次数
                    s = requests.session()
                    s.keep_alive = False  # 设置连接活跃状态

                    url = url_list[i]
                    response = requests.get(url_list[i], headers=headers)
                    content = trafilatura.process_record(response.content.decode("utf-8"))
                    print("正在抓取 " + url)
                    if title_list:
                        if type(title_list[i]) != float:
                            title = title_list[i]

                    if policy_index_list:
                        if type(policy_index_list[i]) != float:
                            policy_index = policy_index_list[i]

                    if public_unit_list:
                        if type(public_unit_list[i]) != float:
                            public_unit = public_unit_list[i]
                            source = public_unit

                    if issued_number_list:
                        if type(issued_number_list[i]) != float:
                            issued_number = issued_number_list[i]

                    if public_time_list:
                        if type(public_time_list[i]) != float:
                            public_time = public_time_list[i]

                    if complete_time_list:
                        if type(complete_time_list[i]) != float:
                            complete_time = complete_time_list[i]

                    info_tuple = (title, policy_index, public_unit, issued_number,
                                  public_time, complete_time, content, url, source, province)
                    self.insert_data(info_tuple)
                except Exception as e:
                    print(e.__cause__)
            exit(1)  # 测试使用

    def insert_data(self, info_tuple: tuple):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param info_tuple:
        :return:
        """
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('USE knowledge')
        try:
            cursor.execute('''INSERT INTO 
            dpp_policy(title, policy_index, public_unit, issued_number, 
            public_time, complete_time, content, url, source, province)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', info_tuple)  # 向远端数据库插入数据
        except Exception as e:
            print(e.__cause__)
            cursor.close()
            return
        self.db.commit()
        cursor.close()  # 游标对象关闭


if __name__ == "__main__":
    tmp = SZFFetcher()
    tmp.loc_policy()
    """
    response = requests.get("http://zjt.hubei.gov.cn/zfxxgk/zc/gfxwj/202111/t20211119_3872076.shtml")
    results = trafilatura.process_record(response.content.decode("utf-8"))
    print(type(results))
    with open("test.txt", "w", encoding="utf-8") as f:
        f.write(results)
    """
    exit(1)
