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

from pymysql.cursors import Cursor
from .data_set import DataSet
from . import conn_util


# 准备使用decorator来封装方法
def datasetmaker(fun):

    def wrapper(*args):
        data_set = DataSet()
        begin = time.time()
        data_set.exec_sql = args[1]
        # data_set.begin_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(begin))
        try:
            data_set.data = fun(args[0], args[1])
        except Exception as e:
            data_set.failure_count += 1
            data_set.message = str(e)
        else:
            data_set.success_count += 1
            data_set.message = 'OK'
        
        end = time.time()
        # data_set.end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))
        data_set.time_diff = round((end - begin), 2)
        # 日志记录暂时空缺
        return data_set

    return wrapper

def datasetbathmaker(function):
    def wrapper(*args):
        data_set = DataSet()
        begin = time.time()
        data_set.exec_sql = args[1]
        
        # 如果是批量处理的话，则返回的是一个tuple
        # (成功计数，失败计数，失败SQLs list)
        result_tuple = function(args[0],args[1])
        data_set.success_count = result_tuple[0]
        data_set.failure_count = result_tuple[1]
        # data_set.exec_sql = result_tuple[2]

        if data_set.failure_count != 0:
            # sqlList 中有执行错误的数据
            data_set.message = f'执行SQL列表中有错误,具体sql:\n{result_tuple[2]}'
        else:
            data_set.message = 'OK'

        return data_set
    return wrapper




# 查询sql语句执行结果封装方法 返回DataSet类型的数据
@datasetmaker
def execute_sql(cursor: Cursor, sql_str: str):
    try:
        cursor.execute(sql_str)
    except Exception as e:
        raise Exception(f"查询SQL失败,具体原因：{e}")

    return cursor.fetchall()


# 非查询类语句执行结果封装
@datasetmaker
def execute_do_sql(cursor: Cursor, sql_str: str):

    try:
        cursor.execute(sql_str)
    except Exception as e:
        raise Exception(f"执行SQL失败,具体原因:{e}")

    return 'OK'


# 非查询类语句批量处理
@datasetbathmaker
def execute_batch_do_sql(cursor: Cursor, sql_list: list):

    failure_count = 0
    success_count = 0
    failure_sqls = []
    for sql in sql_list:
        try:
            cursor.execute(sql)
        except Exception as e:
            # 执行sql失败
            failure_count += 1
            failure_sqls.append(sql)
        else:
            success_count += 1

    return (success_count, failure_count, failure_sqls)


# 执行查询sql语句
def query_sql(sql_str: str):
    connector = None
    cursor = None
    try:

        connector = conn_util.get_connection()  # 获取数据库连接
        cursor = conn_util.get_cursor(connector)  # 获取数据库指针

        data_set = execute_sql(cursor, sql_str)
    except Exception as e:
        print(e)
        # raise e
    else:
        return data_set
    finally:
        conn_util.close_cursor(cursor)
        conn_util.close_connector(connector)


# 执行非查询单条sql语句
def do_sql(sql_str: str):
    connector = None
    cursor = None
    try:
        connector = conn_util.get_connection()  # 获取数据库连接
        cursor = conn_util.get_cursor(connector)  # 获取数据库指针

        data_set = execute_do_sql(cursor, sql_str)
    except Exception as e:
        print(e)
        # raise e
    else:
        return data_set
    finally:
        conn_util.close_cursor(cursor)
        conn_util.close_connector(connector)


# 批量执行非查询类sql语句
def batch_do_sql(sql_list: list):
    connector = None
    cursor = None
    try:
        connector = conn_util.get_connection()  # 获取数据库连接
        cursor = conn_util.get_cursor(connector)  # 获取数据库指针

        data_set = execute_batch_do_sql(cursor, sql_list)
    except Exception as e:
        print(e)
        # raise e
    else:
        return data_set
    finally:
        conn_util.close_cursor(cursor)
        conn_util.close_connector(connector)




