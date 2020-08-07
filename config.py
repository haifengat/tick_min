#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   config.py
@Time    :   2020/07/16 22:07:49
@Author  :   Haifeng
@Contact :   hubert28@qq.com
@Desc    :   
'''

# here put the import lib
import os, sys
# from sqlalchemy import create_engine
# from sqlalchemy.engine import Engine
from color_log import Logger


log = Logger()

tick_csv_gz_path = '/mnt/future_tick_csv_gz'
if 'tick_csv_gz_path' in os.environ:
    tick_csv_gz_path = os.environ['tick_csv_gz_path']

min_csv_gz_path = '/mnt/future_min_csv_gz'
if 'min_csv_gz_path' in os.environ:
    min_csv_gz_path = os.environ['min_csv_gz_path']

# pg_tick_conn = 'postgres://postgres:123456@127.0.0.1:35432/postgres'
# if 'pg_tick_conn' in os.environ:
#     pg_tick_conn = os.environ['pg_tick_conn']

# pg_min_conn = 'postgres://postgres:123456@127.00.1:25432/postgres'
# if 'pg_min_conn' in os.environ:
#     pg_min_conn = os.environ['pg_min_conn']

# en_tick: Engine = create_engine(pg_tick_conn)
# en_min: Engine = create_engine(pg_min_conn)