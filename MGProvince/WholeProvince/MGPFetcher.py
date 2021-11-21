# -*- coding:utf-8 -*-
import json
from json import JSONDecodeError
import time
import pymysql
import requests
from requests import adapters
from requests.packages import urllib3
import trafilatura
from selenium import webdriver
import datetime

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于内蒙古整个自治区进行抓取。
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
            "pageNum": 26,
            "pageSize": 15,
            "sort": "time",
            "suitability": 1,
            "position": 1,
            "searchTag": "zc"
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                          Chrome/94.0.4606.71 Safari/537.36 ',
            "content-type": "application/json"
        }  # 请求头部，用于最基本的反爬如果失败则立即停止说明目标文件非公开
        api_url = "https://www.nmg.gov.cn/nmsearch/openSearch/getZcList?"

        requests.packages.urllib3.disable_warnings()  # 部分网站SSL问题，忽略
        # 将初次爬取到的数据签入success.json中用于观察数据
        # response = requests.get(url=api_url, params=params)
        # print(response.json()["page"]["content"][0])
        # print(response.status_code)
        # exit(1)

        while True:
            preview_data = None
            try:  # 开始爬取政策列表，由于浙江提供了对应的content值是完整的所以不需要跳转对应位置文本过滤
                requests.adapters.DEFAULT_RETRIES = 20  # 设置重连次数
                s = requests.session()
                s.keep_alive = False  # 设置连接活跃状态
                preview_data = requests.get(url=api_url, params=params, headers=headers)  # 开始爬取政策列表
                # 测试代码使用

                """"
                with open("success.json", "w", encoding="utf-8") as f:
                    result = json.dumps(preview_data.json(), indent=4, ensure_ascii=False)
                    f.write(result)
                exit(1)
                """

                preview_data = preview_data.json()
            except Exception:
                time.sleep(1)
                params["pageNum"] += 1  # 向后爬取
                continue

            if preview_data is None:
                params["pageNum"] += 1  # 向后爬取
                continue  # 如果请求返回结果为空则放弃

            if preview_data["data"] is None:  # 爬取内容结束退出循环
                break

            print("正在爬取第" + str(params["pageNum"]) + "页")
            for each_policy in preview_data["data"]["data"]:
                if "zc_doccontent" not in each_policy:
                    continue
                else:
                    self.insert_data(each_policy, "", each_policy["zc_doccontent"])
            params["pageNum"] += 1  # 向后面爬取
            time.sleep(1)

    def insert_data(self, info: json, year: str, content: str):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param year: 政策年份用于核对是否退出循环
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        print("正在抓取年份为" + year + " " + info["docpuburl"])
        # return  # 测试能否通过检验
        publisher = ""  # 提取发文机关
        if "sitedesc" in info:
            publisher = info["sitedesc"]

        file_number = "无发文字号"  # 提取发文字号
        if "docno" in info:
            file_number = info["docno"]
            if file_number is None or file_number == "":
                file_number = "无发文字号"

        policy_id = "无索引号"  # 提取索引号
        if "idxId" in info:
            policy_id = info["idxId"]
            if policy_id is None or policy_id == "":
                policy_id = "无索引号"

        complete_time = "1949-10-01"  # 直接获取成文时间
        if "scrq" in info:
            if info["scrq"] is None or info["scrq"] == "1949-10-01":
                complete_time = ""
            else:
                complete_time = info["scrq"][0:10]

        pub_time = "1949-10-01"  # 直接获取发文时间
        if "docpubtime" in info:
            if info["docpubtime"] is None or info["docpubtime"] == "1949-10-01":
                pub_time = ""
            else:
                pub_time = info["docpubtime"][0:10]

        info_tuple = (info["title"], policy_id, publisher, file_number,
                      pub_time, complete_time, content, info["docpuburl"], publisher, "内蒙古")
        content.encode("utf-8")
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
