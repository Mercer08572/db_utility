# -*- coding: utf-8 -*-
# DateTime  : 2022/1/31 17:15
# Author    : Badbugu17
# File      : DataSet.py
# Software  : PyCharm

# 目前是 。 2023/2/13 我不知道这种形式合适不合适，目前先这样吧。

class DataSet:

    def __init__(self):
        # self.begin_time = ""
        self.exec_sql = None
        self.data = None  # 查询后的数据,如果是增删改,则是OK
        # self.end_time = ""
        self.time_diff = None
        self.failure_count = 0
        self.success_count = 0
        self.message = ''

    def __str__(self):
        return f'begin_time:{self.begin_time}\n' \
               f'exec_sql:{self.exec_sql}\n' \
               f'data:{self.data}\n' \
               f'end_time:{self.end_time}\n' \
               f'time_diff:{self.time_diff}'