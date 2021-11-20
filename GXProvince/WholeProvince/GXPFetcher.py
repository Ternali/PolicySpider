# -*- coding:utf-8 -*-
import json
from json import JSONDecodeError
import time
import pymysql
import requests
from requests import adapters
from requests.packages import urllib3
import trafilatura
import datetime

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于广西整个省进行抓取，广西
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

        params = {
            "index": "file2-index-alias",
            "type": "governmentdocuments",
            "pageNumber": 10001,
            "pageSize": 10,
            "filter[AVAILABLE]": True,
            "filter[fileYear]": 2021,
            "siteId": 14,
            "orderProperty": "PUBDATE",
            "orderDirection": "desc"
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                          Chrome/94.0.4606.71 Safari/537.36 '
        }  # 请求头部，用于最基本的反爬如果失败则立即停止说明目标文件非公开
        api_url = "http://www.gxzf.gov.cn/igs/front/search/list.html?"

        requests.packages.urllib3.disable_warnings()  # 部分网站SSL问题，忽略
        year_params = ["2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]  # 设置爬取年份集合

        for i in range(0, len(year_params)):
            params["filter[fileYear]"] = year_params[i]   # 设置爬取年份参数
            params["pageNumber"] = 1  # 重置爬取开始页数
            while True:
                preview_data = None
                try:  # 开始爬取政策列表，由于浙江提供了对应的content值是完整的所以不需要跳转对应位置文本过滤
                    requests.adapters.DEFAULT_RETRIES = 20  # 设置重连次数
                    s = requests.session()
                    s.keep_alive = False  # 设置连接活跃状态
                    preview_data = requests.get(url=api_url, params=params)  # 开始爬取政策列表
                    if preview_data.status_code == 500:
                        print("当前爬取年份结束跳转下一年开始爬取")
                        break  # 说明数据解析失败，服务器请求失败则跳转下一年份
                    preview_data = preview_data.json()
                # except JSONDecodeError:  # json数据解析失败，由于广西省数据特殊性，部分内容并非能够支持爬取
                    # print("当前爬取年份结束跳转下一年开始")
                    # break  # json数据解析失败说明爬取结束，跳转到下一年份进行爬取数据
                except Exception:
                    time.sleep(1)
                    params["pageNumber"] += 1  # 向后爬取
                    continue

                if preview_data is None:
                    continue  # 如果请求返回结果为空则放弃

                print("正在爬取第" + str(params["pageNumber"]) + "页")
                for each_policy in preview_data["page"]["content"]:
                    if "DOCCONTENT" not in each_policy:
                        continue
                    self.insert_data(each_policy, year_params[i], each_policy["DOCCONTENT"])
                params["pageNumber"] += 1  # 向后面爬取
                time.sleep(1)

    def insert_data(self, info: json, year: str, content: str):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param year: 政策年份用于核对是否退出循环
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        print("正在抓取年份为" + year + " " + info["DOCPUBURL"])
        # return  # 测试能否通过检验
        publisher = ""  # 提取发文机关
        if "publisher" in info:
            publisher = info["publisher"]
        fileNum = ""  # 提取发文字号
        if "fileNum" in info:
            fileNum = info["fileNum"]
        IdxID = ""  # 提取索引号
        if "IdxID" in info:
            IdxID = info["IdxID"]
        trs_warehouse_time = ""  # 直接获取成文时间
        if "trs_warehouse_time" in info:
            trs_warehouse_time = info["trs_warehouse_time"][0:10]
        PUBDATE = ""  # 直接获取发文时间
        if "PUBDATE" in info:
            PUBDATE = info["PUBDATE"][0:10]
        info_tuple = (info["DOCTITLE"], IdxID, publisher, fileNum,
                      PUBDATE, trs_warehouse_time, content, info["DOCPUBURL"], publisher, "广西")
        content.encode("utf-8")
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('USE knowledge')
        try:
            cursor.execute('''INSERT INTO 
            dpp_policy(title, policy_index, public_unit, issued_number, 
            public_time, complete_time, content, url, source, province)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', info_tuple)  # 向远端数据库插入数据
        except Exception as e:
            print("部分键值一定转换问题放弃抓取")
            cursor.close()
            return
        self.db.commit()
        cursor.close()  # 游标对象关闭


if __name__ == "__main__":
    tmp = SZFFetcher()
    tmp.loc_policy()
    """
    response = requests.get("https://www.fujian.gov.cn/zwgk/zfxxgk/szfwj/jgzz/rsbzsjjc/200805/t20080528_1117673.htm")
    results = trafilatura.process_record(response.content.decode("utf-8"))
    print(type(results))
    with open("test.txt", "w", encoding="utf-8") as f:
        f.write(results)
    """
    exit(1)
