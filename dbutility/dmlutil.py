# -*- coding: utf-8 -*-
# DateTime  : 2022/1/31 16:14
# Author    : Badbugu17
# File      : dmlutil.py
# Software  : PyCharm

# 数据库常规语句操作类，增删改查
# 功能1 获取数据表主键值，返回主键值+1 要求： 数据库主键必须为数字类型不设自增(待定)
# 我突然觉得功能1并不是太实用查询一个表的主键，并手动设置主键加一很麻烦，不如直接设置主键自增
# ps: mysql查找一个表的主键: SELECT column_name,is_nullable,data_type,column_key FROM information_schema.columns WHERE table_schema = '' AND table_name = ''
#     column_key 显示 PRI的就为表的主键
import time

import pymysql
from dbutility.DataSet import DataSet
from dbutility import connutil


# 准备使用decorator来封装方法
def datasetmaker(fun):

    def wrapper(*args):
        data_set = DataSet()
        begin = time.time()
        data_set.exec_sql = args[1]
        data_set.begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(begin))
        data_set.data = fun(args[0], args[1])
        end = time.time()
        data_set.end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        data_set.time_diff = round((end - begin), 2)
        # 日志记录暂时空缺
        return data_set

    return wrapper


# 查询sql语句执行结果封装方法 返回DataSet类型的数据
@datasetmaker
def execute_sql(cursor, sql_str: str):

    cursor.execute(sql_str)

    return cursor.fetchall()


# 非查询类语句执行结果封装
@datasetmaker
def execute_do_sql(cursor, sql_str: str):

    cursor.execute(sql_str)

    return 'OK'


# 非查询类语句批量处理
@datasetmaker
def execute_batch_do_sql(cursor, sql_list: list):

    for sql in sql_list:
        cursor.execute(sql)

    return 'OK'


# 执行查询sql语句
def query_sql(sql_str: str):
    connector = None
    cursor = None
    try:

        connector = connutil.get_connector()  # 获取数据库连接
        cursor = connutil.get_cursor(connector)  # 获取数据库指针

        data_set = execute_sql(cursor, sql_str)
    except Exception as e:
        print(e)
        raise e
    else:
        return data_set

    finally:
        connutil.close_cursor(cursor)
        connutil.close_connector(connector)


# 执行非查询单条sql语句
def do_sql(sql_str: str):
    connector = None
    cursor = None
    try:
        connector = connutil.get_connector()  # 获取数据库连接
        cursor = connutil.get_cursor(connector)  # 获取数据库指针

        data_set = execute_do_sql(cursor, sql_str)

    except Exception as e:
        print(e)
        raise e
    else:
        return data_set

    finally:
        connutil.close_cursor(cursor)
        connutil.close_connector(connector)


# 批量执行非查询类sql语句
def batch_do_sql(sql_list: list):
    connector = None
    cursor = None
    try:
        connector = connutil.get_connector()  # 获取数据库连接
        cursor = connutil.get_cursor(connector)  # 获取数据库指针

        data_set = execute_batch_do_sql(cursor, sql_list)

    except Exception as e:
        print(e)
        raise e

    else:
        return data_set

    finally:
        connutil.close_cursor(cursor)
        connutil.close_connector(connector)




