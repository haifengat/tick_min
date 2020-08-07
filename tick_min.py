#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   tick_min.py
@Time    :   2020/07/16 22:07:55
@Author  :   Haifeng
@Contact :   hubert28@qq.com
@Desc    :   从pg中取tick数据后生成min数据
'''

# here put the import lib
import config as cfg
import csv, json, sys, os
from time import sleep
from datetime import datetime, timedelta
import gzip

trade_time = {}
trading_days = []
# 交易开盘前1分钟,小节收盘时间,中间交易所有时间
opens = set()
ends = set()
trading_mins = set()

def init():
    """初始化:取交易日历和品种时间"""
    trading_days.clear()
    with open('/home/calendar.csv') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r['tra'] == 'false':
                continue
            trading_days.append(r['day'])
            
    trade_time.clear()
    tmp = {}
    with open('/home/tradingtime.csv') as f:
        reader = csv.DictReader(f)
        proc_day = {}
        for r in reader:
            # 按时间排序, 确保最后实施的时间段作为依据.
            if r['GroupId'] not in proc_day or r['OpenDate'] > proc_day[r['GroupId']]:
                tmp[r['GroupId']] = r['WorkingTimes']
            proc_day[r['GroupId']] = r['OpenDate']
    # 根据时间段设置,生成 opens; ends; mins盘中时间
    for section  in tmp.values():
        for s in json.loads(section):
            opens.add((datetime.strptime(s['Begin'], '%H:%M:%S') + timedelta(minutes=-1)).strftime('%H:%M:00'))
            ends.add(s['End'])
            t_begin = datetime.strptime('20180101' + s['Begin'], '%Y%m%d%H:%M:%S')
            s_end = datetime.strptime('20180101' + s['End'], '%Y%m%d%H:%M:%S')
            if t_begin > s_end:  # 夜盘
                s_end += timedelta(days=1)
            while t_begin < s_end:
                trading_mins.add(t_begin.strftime('%H:%M:00'))
                t_begin = t_begin + timedelta(minutes=1)
    # for g_id, section  in tmp.items():
    #     opens = []
    #     ends = []
    #     mins = []
    #     for s in json.loads(section):
    #         opens.append((datetime.strptime(s['Begin'], '%H:%M:%S') + timedelta(minutes=-1)).strftime('%H:%M:00'))
    #         ends.append(s['End'])
    #         t_begin = datetime.strptime('20180101' + s['Begin'], '%Y%m%d%H:%M:%S')
    #         s_end = datetime.strptime('20180101' + s['End'], '%Y%m%d%H:%M:%S')
    #         if t_begin > s_end:  # 夜盘
    #             s_end += timedelta(days=1)
    #         while t_begin < s_end:
    #             mins.append(t_begin.strftime('%H:%M:00'))
    #             t_begin = t_begin + timedelta(minutes=1)
    #     trade_time[g_id] = {'Opens': opens, 'Ends': ends, 'Mins': mins}


def csv_tick_min(tradingday: str):
    """从csv文件中读取tick后合成分钟

    Args:
        tradingday (str): 交易日
    """
    cfg.log.info(f'{tradingday} starting...')
    # {合约:{分钟:dict分钟数据}}
    inst_mins = {}
    # TradingDay,InstrumentID,UpdateTime,UpdateMillisec,ActionDay,LowerLimitPrice,UpperLimitPrice,BidPrice1,AskPrice1,AskVolume1,BidVolume1,LastPrice,Volume,OpenInterest,Turnover,AveragePrice
    with gzip.open(os.path.join(cfg.tick_csv_gz_path, f'{tradingday}.csv.gz')) as gz_file:
        line = gz_file.readline().decode(encoding='utf8') # 第一行为标题
        line = gz_file.readline().decode(encoding='utf8')
        while len(line) > 0:
            TradingDay,InstrumentID,UpdateTime,UpdateMillisec,ActionDay,LowerLimitPrice,UpperLimitPrice,BidPrice1,AskPrice1,AskVolume1,BidVolume1,LastPrice,Volume,OpenInterest,Turnover,AveragePrice = line.split(',')
            LastPrice = round(float(LastPrice), 4)
            ut = UpdateTime[0:6] + '00'
            # 合成分钟
            if InstrumentID not in inst_mins:
                inst_mins[InstrumentID] = {}
            # 时间:K线
            dt_mins = inst_mins[InstrumentID]
            # 新数据
            dt = f'{ActionDay[0:4]}-{ActionDay[4:6]}-{ActionDay[6:]} {ut}'
            if dt not in dt_mins:
                cur_min = {}
                cur_min['Instrument'] = InstrumentID
                cur_min['DateTime'] = dt
                cur_min['TradingDay'] = tradingday
                cur_min['Low'] = cur_min['Close'] = cur_min['High'] = cur_min['Open'] = LastPrice
                cur_min['OpenInterest'] = float(OpenInterest)
                cur_min['Volume'] = 0
                cur_min['pre_vol'] = int(Volume)
                dt_mins[dt] = cur_min
            else: # 更新
                cur_min = dt_mins[dt]
                cur_min['High'] = max(cur_min['High'], LastPrice)
                cur_min['Low'] = min(cur_min['Low'], LastPrice)
                cur_min['Close'] = LastPrice
                cur_min['OpenInterest'] = float(OpenInterest)
                cur_min['Volume'] = int(Volume) - cur_min['pre_vol']
            line = gz_file.readline().decode(encoding='utf8')

    # 合成的分钟入库
    with gzip.open(os.path.join(cfg.min_csv_gz_path, f"{tradingday}.gz"), 'w') as f_min:
        f_min.write(bytes('DateTime\tInstrument\tOpen\tHigh\tLow\tClose\tVolume\tOpenInterest\n', encoding='utf-8'))
        for mins in inst_mins.values():
            for m in sorted(mins.values(), key=lambda x: x['DateTime']):
                # 处理开/收时间
                if m['High'] == m['Low']: # 开盘前竞价
                    if m['DateTime'][-8:] in opens:
                        continue
                    elif m['DateTime'][-8:] in ends: # (小节)收盘
                        continue
                    elif m['DateTime'][-8:] not in trading_mins: # 非交易时间
                        continue
                    else:
                        continue
                f_min.write(bytes(f"{m['DateTime']}\t{m['Instrument']}\t{m['Open']}\t{m['High']}\t{m['Low']}\t{m['Close']}\t{m['Volume']}\t{m['OpenInterest']}\n", encoding='utf-8'))
    # 完成后改名,避免后续操作读到未完成的数据
    os.rename(os.path.join(cfg.min_csv_gz_path, f"{tradingday}.gz"), os.path.join(cfg.min_csv_gz_path, f"{tradingday}.csv.gz"))
    cfg.log.info(f'{tradingday} finish.')


if __name__ == "__main__":
    
    init()
    maxday_min = ''
    if not os.path.exists(cfg.min_csv_gz_path):
        os.mkdir(cfg.min_csv_gz_path)
    else:
        mins = [f.split('.')[0] for f in os.listdir(cfg.min_csv_gz_path)]
        if len(mins) > 0:
            maxday_min = max(mins)

    ticks = [f.split('.')[0] for f in os.listdir(cfg.tick_csv_gz_path) if f > maxday_min]
    # for ... do tick_min
    for d in ticks:
        csv_tick_min(d)
    maxday_min = max(ticks)
    next_day = trading_days[trading_days.index(maxday_min)+1]
    while True:
        if os.path.exists(os.path.join(cfg.tick_csv_gz_path, f'{next_day}.csv.gz')):
            csv_tick_min(next_day)
            next_day = trading_days[trading_days.index(next_day)+1]
        sleep(60 * 10)


# def read_ticks_pg(tradingday:str):
#     """取pg中的tick数据

#     Args:
#         tradingday (str): 交易日
#     """
#     cfg.log.info(tradingday)
#     init()
#     # {合约:{分钟:dict分钟数据}}
#     inst_mins = {}

#     sqlstr = f'select "Instrument" , "Actionday" , "UpdateTime" , "UpdateMillisec" , "LastPrice" , "Volume" , "OpenInterest" from future_tick."{tradingday}" order by "Actionday" , "UpdateTime" , "UpdateMillisec" '
#     conn = cfg.en_tick.raw_connection()
#     cursor = conn.cursor()
#     cursor.execute(sqlstr)
#     r = cursor.fetchone()
#     while r is not None:
#         Instrument, Actionday, UpdateTime, UpdateMillisec, LastPrice, Volume , OpenInterest = r
#         LastPrice = round(LastPrice, 4)
#         ut = UpdateTime[0:6] + '00'
#         # 合成分钟
#         if Instrument not in inst_mins:
#             inst_mins[Instrument] = {}
#         # 时间:K线
#         dt_mins = inst_mins[Instrument]
#         # 新数据
#         dt = f'{Actionday[0:4]}-{Actionday[4:6]}-{Actionday[6:]} {ut}'
#         if dt not in dt_mins:
#             cur_min = {}
#             cur_min['Instrument'] = Instrument
#             cur_min['DateTime'] = dt
#             cur_min['TradingDay'] = tradingday
#             cur_min['Low'] = cur_min['Close'] = cur_min['High'] = cur_min['Open'] = LastPrice
#             cur_min['OpenInterest'] = OpenInterest
#             cur_min['Volume'] = 0
#             cur_min['pre_vol'] = Volume
#             dt_mins[dt] = cur_min
#         else: # 更新
#             cur_min = dt_mins[dt]
#             cur_min['High'] = max(cur_min['High'], LastPrice)
#             cur_min['Low'] = min(cur_min['Low'], LastPrice)
#             cur_min['Close'] = LastPrice
#             cur_min['OpenInterest'] = OpenInterest
#             cur_min['Volume'] = Volume - cur_min['pre_vol']
#         r = cursor.fetchone()

#     # 合成的分钟入库
#     conn_min = cfg.en_min.raw_connection()
#     cursor = conn_min.cursor()
#     output = StringIO()
#     f = open("./t.csv", 'w+')
#     for mins in inst_mins.values():
#         for m in sorted(mins.values(), key=lambda x: x['DateTime']):
#             # 处理开/收时间
#             if m['High'] == m['Low']: # 开盘前竞价
#                 if m['DateTime'][-8:] in opens:
#                     continue
#                 elif m['DateTime'][-8:] in ends: # (小节)收盘
#                     continue
#                 elif m['DateTime'][-8:] not in trading_mins: # 非交易时间
#                     continue
#                 else:
#                     continue
#             if m['Instrument'] == 'ZC005':
#                 print(m['High'])
#             output.write(f"{m['DateTime']}\t{m['Instrument']}\t{m['Open']}\t{m['High']}\t{m['Low']}\t{m['Close']}\t{m['Volume']}\t{m['OpenInterest']}\t{m['TradingDay']}\n")
#             f.write(f"{m['DateTime']}\t{m['Instrument']}\t{m['Open']}\t{m['High']}\t{m['Low']}\t{m['Close']}\t{m['Volume']}\t{m['OpenInterest']}\t{m['TradingDay']}\n")
#     output.seek(0)
#     cursor.copy_from(output, 'future.future_min')
#     conn_min.commit()
#     cursor.close()

# if __name__ == "__main__":
    
#     # 取future_min max(tradingday)=max_day
#     conn_min = cfg.en_min.raw_connection()
#     cursor = conn_min.cursor()
#     sqlstr = 'select max("TradingDay" ) from future.future_min'
#     cursor.execute(sqlstr)
#     maxday_min = cursor.fetchone()[0]
#     if maxday_min is None:
#         maxday_min = ''

#     while True:
#         # 取future_tick.${tablename} > max_day
#         conn_tick = cfg.en_tick.raw_connection()  # engine 是 from sqlalchemy import create_engine
#         cursor = conn_tick.cursor()
#         sqlstr = f"select tablename from pg_catalog.pg_tables where schemaname = 'future_tick' and tablename > '{maxday_min}'"
#         cursor.execute(sqlstr)
#         maxday_tick = cursor.fetchall()
#         if maxday_tick is None:
#             cfg.log.error('there is not tick data in future_tick schema')
#             sys.exit(-1)
#         # for ... do tick_min
#         for d in [d[0] for d in maxday_tick if d[0] > maxday_min]:
#             read_ticks_pg(d)
#             maxday_min = max(maxday_min, d)
#         sleep(60 * 10)

