# -*- coding: utf-8 -*-
# DateTime  : 2022/1/30 19:50
# Author    : Badbugu17
# File      : connutil.py
# Software  : PyCharm

import pymysql
from pymysql.cursors import Cursor
from pymysql.connections import Connection
import configparser


# 获取数据库信息，创建连接并返回dbconn，目前是直接获取的样式，之后会改成从配置文件中获取
def get_connection():

    config = configparser.ConfigParser()
    config.read('../config/conf.ini')

    try:
        dbconn = pymysql.connect(
            host=config['DataBase']['host'],
            # port=3306,
            user=config['DataBase']['user'],
            password=config['DataBase']['password'],
            database=config['DataBase']['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(e)
        # raise e
    else:
        return dbconn


# 获取指针
def get_cursor(dbconn: Connection):
    return dbconn.cursor()


# 关闭数据库指针
def close_cursor(cursor: Cursor):
    if cursor is not None:
        cursor.close()


# 关闭数据库连接，在进行数据库操作之后，必须关闭数据库连接
def close_connector(dbconn: Connection):
    if dbconn is not None:
        dbconn.close()


def get_database_conf(name:str):
    config = configparser.ConfigParser()
    config.read('config/conf.ini')
    
    return config.get('DataBase',name)