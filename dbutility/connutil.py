# -*- coding: utf-8 -*-
# DateTime  : 2022/1/30 19:50
# Author    : Badbugu17
# File      : connutil.py
# Software  : PyCharm

import pymysql


# 获取数据库信息，创建连接并返回dbconn，目前是直接获取的样式，之后会改成从配置文件中获取
def get_connector():

    try:
        dbconn = pymysql.connect(
            host='',
            # port=3306,
            user='',
            password='',
            database='',
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(e)
        raise e
    else:
        return dbconn


# 获取指针
def get_cursor(dbconn):
    return dbconn.cursor()


# 关闭数据库指针
def close_cursor(cursor):
    if cursor is not None:
        cursor.close()


# 关闭数据库连接，在进行数据库操作之后，必须关闭数据库连接
def close_connector(dbconn):
    if dbconn is not None:
        dbconn.close()
