# -*- coding: utf-8 -*-
# DateTime  : 2022/1/31 16:14
# Author    : Badbugu17
# File      : DmlUtil.py
# Software  : PyCharm

# 数据库常规语句操作类，增删改查
# 功能1 获取数据表主键值，返回主键值+1 要求： 数据库主键必须为数字类型不设自增(待定)
# 我突然觉得功能1并不是太实用查询一个表的主键，并手动设置主键加一很麻烦，不如直接设置主键自增
# ps: mysql查找一个表的主键: SELECT column_name,is_nullable,data_type,column_key FROM information_schema.columns WHERE table_schema = '' AND table_name = ''
#     column_key 显示 PRI的就为表的主键
import time

import pymysql

from xyz.badbugu.DBUtil.ConnUtil import ConnUtil
from xyz.badbugu.DBUtil.DataSet import DataSet


# 查询sql语句执行结果封装方法 返回DataSet类型的数据
def executeSql(cursor: pymysql.cursors.Cursor, sql: str):
    #
    dataSet = DataSet()
    dataSet.type = 1.1
    # 设置开始查询时间
    dataSet.beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cursor.execute(sql)
    # 设置查询结束时间
    dataSet.endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    dataSet.data = cursor.fetchall()

    return dataSet


# 非查询类语句执行结果封装
def executeDoSql(cursor: pymysql.cursors.Cursor, sql: str):
    dataSet = DataSet()
    dataSet.type = 1.2
    # 设置开始查询时间
    dataSet.beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cursor.execute(sql)
    # 设置查询结束时间
    dataSet.endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    dataSet.data = 'OK'

    return dataSet

# 非查询类语句批量处理
def executeBatchDoSql(cursor: pymysql.cursors.Cursor, sqlList: list):

    dataSet = DataSet()
    dataSet.type = 1.2
    # 设置开始执行时间
    dataSet.beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for sql in sqlList:
        cursor.execute(sql)
    dataSet.endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    dataSet.data = 'OK'

    return dataSet

class DmlUtil:
    # # 获取最大主键
    # def getMaxPrimaryKeyValue(self,tablename: str):
    #     cursor = ConnUtil._getConnector()

    # 实例化数据库连接类
    connUtil = ConnUtil()

    # 执行查询sql语句
    def querySql(self, sql: str):
        try:

            connector = self.connUtil.getConnector()  # 获取数据库连接
            cursor = self.connUtil.getCursor(connector)  # 获取数据库指针

            dataSet = self.executeSql(cursor, sql)
        except Exception as e:
            print(e)
            raise e
        else:
            return dataSet

        finally:
            self.connUtil.closeConnector(connector)

    # 执行非查询单条sql语句
    def doSql(self, sql: str):
        try:
            connector = self.connUtil.getConnector()  # 获取数据库连接
            cursor = self.connUtil.getCursor(connector)  # 获取数据库指针

            dataSet = self.executeDoSql(cursor, sql)

        except Exception as e:
            print(e)
            raise e
        else:
            return dataSet

        finally:
            self.connUtil.closeConnector(connector)

    # 批量执行非查询类sql语句
    def batchDoSql(self, sqlList: list):
        try:
            connector = self.connUtil.getConnector()  # 获取数据库连接
            cursor = self.connUtil.getCursor(connector)  # 获取数据库指针

            dataSet = self.batchDoSql(cursor, sqlList)

        except Exception as e:
            print(e)
            raise e

        else:
            return dataSet

        finally:
            self.connUtil.closeConnector(connector)




