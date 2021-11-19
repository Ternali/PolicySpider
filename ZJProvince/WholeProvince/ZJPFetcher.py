# -*- coding:utf-8 -*-
import json
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
本代码为对于浙江省整个省进行抓取，由于浙江省的文件返回的特殊性质判断是否初次爬取没多大用处
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
            "websiteid": 330000000000000,
            "pg": 30,
            "p": 1,
            "cateid": 657,
            "_cus_lq_filenumber": None,
            #  "q": "温州市龙湾区人民政府办公室关于印发龙湾区城区功能转变和产业结构优化实施方案的通知"
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                                          Chrome/94.0.4606.71 Safari/537.36 '
        }  # 请求头部，用于最基本的反爬如果失败则立即停止说明目标文件非公开
        api_url = "https://search.zj.gov.cn/jsearchfront/interfaces/search.do?"

        requests.packages.urllib3.disable_warnings()  # 部分网站SSL问题，忽略
        year_params = ["2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]  # 设置爬取年份集合

        for i in range(0, len(year_params)):
            params["_cus_lq_filenumber"] = "〔" + year_params[i] + "〕"  # 设置爬取年份参数
            params["p"] = 1  # 重置爬取开始页数
            response = requests.post(url=api_url, params=params).json()  # 初次请求接口设置当年政策数用于停止爬取
            policy_num = response["total"]
            policy_counter = 0
            while True:
                if policy_counter >= policy_num:
                    break  # 说明政策数量和当前请求年份返回的数量一致跳出循环
                preview_data = None
                try:  # 开始爬取政策列表，由于浙江提供了对应的content值是完整的所以不需要跳转对应位置文本过滤
                    requests.adapters.DEFAULT_RETRIES = 20  # 设置重连次数
                    s = requests.session()
                    s.keep_alive = False  # 设置连接活跃状态
                    preview_data = requests.post(url=api_url, params=params).json()  # 开始爬取政策列表
                except Exception:
                    time.sleep(1)
                if preview_data is None:
                    continue  # 如果请求返回结果为空则放弃
                policy_counter += len(preview_data["dataResults"])
                print("正在爬取第" + str(params["p"]) + "页")
                for each_policy in preview_data["dataResults"]:
                    if "content" not in each_policy["data"]:
                        continue
                    self.insert_data(each_policy["data"], year_params[i], each_policy["data"]["content"])
                params["p"] += 1  # 向后面爬取
                time.sleep(2)

    def insert_data(self, info: json, year: str, content: str):
        """
        依赖初始化中连接好的数据库对象，使用cursor游标对象连接到远端数据库，并执行后续插入数据等操作。
        :param year: 政策年份用于核对是否退出循环
        :param info: 政策的相关信息的json存储，原名为policy_info过长不便于编写故简写info
        :param content: 对应政策的具体文本内容
        :return:
        """
        print("正在抓取年份为" + year + " " + info["url"])
        # return  # 测试能否通过检验
        fbjg = ""  # 提取发文机关
        if "fbjg" in info:
            fbjg = info["fbjg"]
        filenumber = ""  # 提取发文字号
        if "filenumber" in info:
            filenumber = info["filenumber"]
        xxgkindexcode = ""  # 提取索引号
        if "xxgkindexcode" in info:
            xxgkindexcode = info["xxgkindexcode"]
        compaltedate = ""  # 解析成文时间，将时间戳转为%Y-%m-%d格式
        if "compaltedate" in info and "compaltedate" != '':
            try:
                compaltedate = time.strftime(time.strftime("%Y-%m-%d", time.localtime(float(info["compaltedate"][0:10]))))
            except Exception:
                return
        deploytime = ""  # 解析发文时间，将时间转成同样的格式
        if "deploytime" in info and "deploytime" != '':
            try:
                deploytime = time.strftime(time.strftime("%Y-%m-%d", time.localtime(float(info["deploytime"][0:10]))))
            except Exception:
                return

        info_tuple = (info["title"], xxgkindexcode, fbjg, filenumber,
                      deploytime, compaltedate, content, info["url"], fbjg, "浙江")
        content.encode("utf-8")
        cursor = self.db.cursor()  # 创建游标对象
        cursor.execute('USE knowledge')
        try:
            cursor.execute('''INSERT INTO 
            dpp_policy(title, policy_index, public_unit, issued_number, 
            public_time, complete_time, content, url, source, province)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', info_tuple)  # 向远端数据库插入数据
        except Exception:
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
