# -*- coding:utf-8 -*-
import json
from json import JSONDecodeError
import time

import pymysql
import requests
from bs4 import BeautifulSoup
from requests import adapters
from requests.packages import urllib3
import trafilatura
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

from selenium.webdriver.support.wait import WebDriverWait

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
本代码为对于河北整个省进行抓取。
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

        api_url = "http://www.hbzwfw.gov.cn/zwbjsearchfront/interfaces/cateSearch.do"
        pre_url = "http://www.hbzwfw.gov.cn/zwbjsearchfront/"
        params = {
            "websiteid": 130000000000000,
            "p": 1,
            "q": "通知",
            "pg": 10,
            "cateid": 12,
            "tpl": 7,
            "begin": "20190101",
            "end": "20211122",
            "sortType": 1,
            "checkError": 1,
            "word": "通知",
            "timetype": 5
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            # 'Cookie': 'SESSION=NjVlNDE2MzAtZDk0ZC00MmI3LWI3YWEtYmI0YTVkNzRiMDk2'
        }

        requests.packages.urllib3.disable_warnings()  # 部分网站SSL问题，忽略
        key_params = ["通知", "决议", "决定", "命令", "公报", "公告", "通告", "意见",
                      "通报", "报告", "请示", "批复", "议案", "函", "纪要"]
        for k in range(0, len(key_params)):
            policy_counter = 0
            preview_data = requests.post(url=api_url, data=params, headers=headers).json()
            total_num = preview_data["total"]
            while True:
                if policy_counter > total_num:
                    break
                preview_data = None
                try:  # 开始爬取政策列表
                    requests.adapters.DEFAULT_RETRIES = 20  # 设置重连次数
                    s = requests.session()
                    s.keep_alive = False  # 设置连接活跃状态
                    preview_data = requests.post(url=api_url, data=params, headers=headers)  # 开始爬取政策列表
                    # 测试代码使用
                    """"
                    with open("success.json", "w", encoding="utf-8") as f:
                        result = json.dumps(preview_data.json(), indent=4, ensure_ascii=False)
                        f.write(result)
                    exit(1)
                    """

                    if preview_data.text == "":
                        break  # 返回数据为空说明爬取结束
                    preview_data = preview_data.json()
                except Exception as e:
                    print(e.__cause__)
                    time.sleep(1)
                    params["p"] += 1  # 向后爬取
                    continue

                print("正在爬取第" + str(params["p"]) + "页")
                policy_counter += len(preview_data)
                for each_policy in preview_data["result"]:
                    try:
                        bs = BeautifulSoup(each_policy, "html.parser")  # 使用BeautifulSoup纠正格式
                        bs.prettify()
                        title = bs.find("div", attrs={"class": "jcse-news-title zwfwzc_title"}).text  # 查找对应标签
                        pub_time = bs.find("span", attrs={"class": "jcse-news-date zwfwzc_pubtime"}).text[3:]  # 查找对应标签
                        link = bs.find("a").get('href')  # 获得对应链接并组装
                        link = unquote(link)  # 转换编码
                        link = link.replace("visit/link.do?url=", "")  # 去除无用信息
                        chrome = webdriver.Chrome()
                        element = WebDriverWait(chrome, 30)
                        # element.until(EC.presence_of_element_located((By.CLASS_NAME, 'ivu-card-body')))
                        chrome.get(link)
                        response = chrome.page_source
                        results = trafilatura.process_record(response)
                        info = (title, "无索引号", "无发文机关", "无发文字号", pub_time, pub_time, results, link, "河北", "河北")
                        self.insert_data(info)
                    except Exception as e:
                        print(e.__cause__)
                        continue
                params["p"] += 1  # 向后面爬取
                time.sleep(1)

    def insert_data(self, info: tuple):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param year: 政策年份用于核对是否退出循环
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        print("正在抓取 " + info[7])
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('USE knowledge')
        try:
            cursor.execute('''INSERT INTO 
            dpp_policy(title, policy_index, public_unit, issued_number, 
            public_time, complete_time, content, url, source, province)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', info)  # 向远端数据库插入数据
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
