import json
import re
import time
import bs4
import requests

"""
大致方法划分为初次定位文件、非初次定位文件、捕获文件涉及到文件爬取需要更新问题，现需决定将爬取到的最新文
标题存入到文档中用于比较，在启动本类中的定位文件为区分是否初次定位，需要将相应的数据存入到对应的文档中用
来判断是否初次，由于抓取政府文件不能抓取频率过快决定每次请求获得部分文档后立即开始对文档的处理。
"""


class ZJProvinceFetcher:
    def __init__(self):
        """
        file_count用于统计获得的文件数量，用于初次退出爬取进程
        is_first用于判断是否初次定位文件，从判断是否初次文档record.json读取
        policy_num用于统计浙江省政策文件数量
        policy_title_store用于记录获得政策文件标题，并用于后续粗略判断是否有新政策依据，从record.json中读取
        """
        self.file_count = 0
        self.is_first = False
        self.policy_num = 0
        self.policy_title_store = []

    def judge_first(self):
        """
        通过读取record.txt内容，其中第一行为最新政策标题，第二行为是否初次定位
        若为初次定位则读取内容为None、True和0，否则为对应标题、False和既有政策数量
        :return:
        """
        with open("record.json", "r") as f:
            tmp_dict = json.loads(f.read())  # 将是否初次Flag，爬取政策标题以及爬取政策数量读取
            self.is_first = tmp_dict['is_first']
            self.policy_num = tmp_dict['policy_num']
            self.policy_title_store = tmp_dict['policy_title_store']
            f.close()

    def make_decision(self):
        """
        用于判断是否初次定位政策，并执行后续的操作定位文档和非初次定位文档
        :return:
        """
        if self.is_first:
            self.first_loc_policy()
        else:
            self.first_loc_policy()
            # loc_policy()还未解决版本问题

    def first_loc_policy(self):
        """
        初次定位文档，需要结合政策最大数量以作为爬取结束的标志，这里需要注意的是由于浙江省
        文件库对于查找页数超出应用的返回内容为最后一页的数据，比较难以终止，故决定采用计数
        的方式，计算爬取的数量是否等于最初获得政策最大数量作为终止标志，注意爬取政府政策涉
        及保密条例，爬取速率应降低保证人身安全。
        :return:
        """
        self.is_first = False
        params = {
            'websiteid': 330000000000000,
            'p': 1,
            'cateid': 657,
            'tpl': 1581,
            'pg': 10
        }  # 请求参数，其中p为页数，pg为每页请求政策数量10较为合适
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  \
                          Chrome/94.0.4606.71 Safari/537.36 '
        }  # 请求头部
        url = 'http://search.zj.gov.cn/jsearchfront/interfaces/search.do?'
        preview_data = requests.get(url=url, params=params, headers=headers).json()  # 捕获json数据
        policy_max_num = preview_data['total']  # 初次需要额外记录最多政策数量

        while True:  # 采用获取一次政策列表就记录对应政策文件来降低请求接口速率，避免吃席
            response = requests.get(url=url, params=params, headers=headers)  # 请求接口
            response.encoding = response.apparent_encoding  # 编码转换
            preview_data = response.json()  # 捕获json数据
            if self.file_count > policy_max_num:  # 说明初次定位所有内容已经结束
                break
            self.file_count += len(preview_data['dataResults'])

            for each_policy in preview_data['dataResults']:  # 开始记录获取到的文档内容
                if self.is_policy(each_policy):
                    self.policy_num += 1  # 用于记录爬取到的符合条件的政策数量
                    self.record_policy(each_policy, params['p'])
                    self.write_to_file()

            time.sleep(2.9)  # 线程睡眠，避免请求频率过快导致服务器崩溃然后吃席
            params['p'] += 1  # 实现请求页面跳转
        return

    def loc_policy(self):
        return

    @staticmethod
    def is_policy(policy_json: json) -> bool:
        """
        用于判断是否需要的政策性文件
        :param policy_json:
        :return:
        """
        if 'fbjg' not in policy_json['data']:  # 用于判断是否政策性文件，是否缺失发文机关
            return False
        if 'filenumber' not in policy_json['data']:  # 用于判断是否政策性文件，是否缺失发文字号
            return False
        if len(policy_json['data']['filenumber']) == 1 or len(policy_json['data']['fbjg']) == 1:
            return False
        return True

    def record_policy(self, policy_json: json, page: int):
        """
        将一系列数据插入到字典中,并转换为json格式数据写入文件
        :param policy_json: 当前政策所在的json内容
        :param page: 当前政策所在页数
        :return:
        """
        policy_dict = {}
        policy_dict.update({'title': policy_json['data']['title']})  # 插入政策标题
        self.policy_title_store.append(policy_json['data']['title'])
        policy_dict.update({'fbjg': policy_json['data']['fbjg']})  # 插入政策发文机关
        policy_dict.update({'filenumber': policy_json['data']['filenumber']})  # 插入政策发文字号
        policy_filenumber = policy_json['data']['filenumber']
        publication_time = re.findall('art/(.*)/art', policy_json['data']['url'])[0]  # 通过正则表达式记录发文时间
        policy_dict.update({'time': publication_time})
        policy_dict.update({'content': ''})

        response = requests.get(policy_json['data']['url'])  # 请求政策所在网页获取单页信息
        content = bs4.BeautifulSoup(response.content.decode('utf8'), 'lxml')

        p_elements = content.find_all('p')
        for p_element in p_elements:
            if p_element.text is None:
                continue
            policy_dict['content'] += p_element.text
            policy_dict['content'] += '\n'
        result = json.dumps(policy_dict, indent=4, ensure_ascii=False)
        print("正在爬取第" + str(self.policy_num) + "条有效政策" + ",发文字号为: " + policy_filenumber
              + "处在第" + str(page) + "页")  # 输出信息

        with open("F:\\PolicyPile\\ZJProvince\\ProvinceGovernment\\"+policy_dict['title'] + '.json',
                  "w", encoding="utf8") as f:
            f.write(result)
            print("爬取内容已经结束, 发文字号: " + policy_filenumber)

        # f = open("F:\\PolicyPile\\ZJProvince\\ProvinceGovernment\\"+policy_dict['title'] + '.json', "w",
        # encoding="utf8") f.write(result) f.close() print("爬取内容已经结束, 发文字号: " + policy_filenumber)
        time.sleep(0.1)

    def write_to_file(self):
        tmp_dict = {}
        tmp_dict.update({'is_first': self.is_first})
        tmp_dict.update({'policy_num': self.policy_num})
        tmp_dict.update({'policy_title_store': self.policy_title_store})
        with open("record.json", "w", encoding='utf8') as f:
            f.write(json.dumps(tmp_dict, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    test = ZJProvinceFetcher()
    test.first_loc_policy()
