# -*- coding:utf-8 -*-
# date:2017-7-13
# author:Alex

import pymysql
import datetime

class get_mysql(object):
    def __init__(self,dbname,key,city):
        self.dbname = dbname
        self.T = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M")
        # 数据库表格的名称
        self.table_name = "{}_{}_{}".format(self.T,key,city)
        # 链接数据库
        self.conn = pymysql.connect(
            host="localhost",
            port=3306,
            user='root',
            password='root',
            db=self.dbname,
            charset='utf8'
        )
        # 获取游标
        self.cursor = self.conn.cursor()

    def create_table(self):
        '''创建表格，创建失败就不创建了'''
        sql = '''CREATE TABLE `{tbname}` (
        {job_name} varchar(120) not null,
        {gs_name} varchar(120),
        {job_gz} varchar(30),
        {job_dd} varchar(200),
        {gs_gm} char(20),
        {cs} char(30),
        {jy} char(30),
        {xlyq} char(20),
        {gs_info} varchar(250),
        {job_link} varchar(250),
        {rzlc} char(50),
        {create_date} char(25)
        )'''
        try:
            self.cursor.execute(sql.format(tbname=self.table_name,job_name="职位名称",gs_name="公司名称",job_gz="职位工资",
                                           job_dd="公司地址",gs_gm="公司规模",cs="城市",xlyq="学历",jy="经验",gs_info="公司信息",
                                           job_link="招聘链接",rzlc="融资轮次",create_date="发布时间"))
        except Exception as e:
            print("创建新的表格失败，原因：",e)
        else:
            self.conn.commit()
            print("创建了一个新的表格,名称是{}".format(self.table_name))

    def insert_data(self,data):
        '''插入数据，执行插入语句失败就回滚，执行成功才提交'''
        sql = '''INSERT INTO `{tbname}` VALUES('{job_name}','{gs_name}','{job_gz}','{job_dd}','{gs_gm}','{cs}','{xlyq}','{jy}',
        '{gs_info}','{job_link}','{rzlc}','{create_date}')'''
        try:
            self.cursor.execute(sql.format(tbname=self.table_name,job_name=data["job_name"],gs_name=data["gs_name"],
                                           job_gz=data["job_gz"],job_dd=data["job_dd"],gs_gm=data["gs_gm"],cs=data["cs"],xlyq=data["xlyq"],
                                           jy=data["jy"],gs_info=data["gs_info"],job_link=data["job_link"],rzlc=data["rzlc"],
                                           create_date=data["create_date"]))
        except Exception as e:
            self.conn.rollback()
            print("插入数据失败，原因：",e)
        else:
            self.conn.commit()
            print("插入一条数据成功")

    # 关闭游标和连接
    def close_table(self):
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    data = {'job_name': '程序员', 'gs_name': '神码', 'job_gz': '1500', 'job_dd': '深圳', 'gs_gm': '150', 'cs': '深圳', 'xlyq': '本科', 'jy': '三年', 'gs_info': '加钱', 'job_link': 'www.dchealth.com', 'rzlc': 'D轮', 'create_date': '昨天'}
    ms = get_mysql("lagou","测试key","测试city")
    ms.create_table()
    ms.insert_data(data)
    ms.close_table()

