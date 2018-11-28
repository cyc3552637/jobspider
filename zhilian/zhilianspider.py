# -*- coding:utf-8 -*-
# date:2017-7-13
# author:Alex

import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import quote
from zhilian.savedata import get_Mysql



class mySpider(object):
    def __init__(self,daname,mykey,mycity):
        self.dbname = daname
        self.key = mykey
        self.city = mycity
        self.start_url = "https://fe-api.zhaopin.com/c/i/sou?kt=3&cityId={}&kw={}".format(quote(self.city), quote(self.key))
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        }
        self.mysql = get_Mysql(self.dbname, self.key, self.city)
        self.mysql.create_table()

    # 获取一页的招聘信息,并判断是否有下一页的链接，递归爬取所有页的信息
    def get_one_html(self,url,start):
        html = requests.get(url+"&start={}pageSize=60".format(start),headers = self.headers).json()
        data = {}
        for i in html['data']['results']:
            # 招聘链接
            data["job_link"] = i['positionURL']
            # 职位名称
            data["job_name"] = i['jobName']
            # 反馈率
            data["fk_lv"] = i['feedbackRation']
            # 公司名称
            data["gs_name"] = i['company']['name']
            # 公司链接
            data["gs_link"] = i['company']['url']
            # 职位月薪
            data["job_gz"] = i['salary']
            # 工作地点
            data["job_dd"] = i['city']['items'][0]['name']
            # 发布日期
            data["create_date"] = i['updateDate']
            # 公司性质
            data["gs_xz"] = i['company']['type']['name']
            # 公司规模
            data["gs_gm"] = i['company']['size']['name']
            # 学历/经验要求
            data["xlyq"] = i['eduLevel']['name']
            data["jy"] = i['workingExp']['name']
            print(data)
            self.mysql.insert_data(data)
        return html['data']['numFound']






    def main(self):
        '''尝试爬信息并保存到数据库，若爬虫失败也要关闭数据库连接'''
        try:
            start = 0
            page = 1
            while True:
                numFound=self.get_one_html(self.start_url,start)
                print('第{0}页'.format(page))
                if start < numFound:
                    start += 60
                    page += 1
                    time.sleep(0.5)
                else:
                    break

        except Exception as e:
            print(e)
        finally:
            self.mysql.close_table()

if __name__ == '__main__':
    t = time.time()
    # 列表循环创建表格
    jobs = ["python","java"]
    citys = ["深圳","武汉"]
    for i in jobs:
        for j in citys:
            s = mySpider("zhilian_jobs",i,j)
            s.main()
    print("耗时：{:.2f}秒".format(float(time.time()-t)))

