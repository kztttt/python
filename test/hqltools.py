#!/usr/bin/env python
# -*-coding:utf-8 -*-
import time
import math
import os, sys
from thrift import Thrift
from hive_service import ThriftHive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service.ttypes import HiveServerException

from log4py import logger,lpath
from os.path import join
ISOTIMEFORMAT = '%Y-%m-%d %X'
ips='10.0.88.249'
# 执行HQL，没有返回结果
def HiveExe(hql, name, dates):
    lock_file = join(lpath, name + '_' + dates + '.lock')
    try:
        transport = TSocket.TSocket(ips, 10001) 
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ThriftHive.Client(protocol)
        transport.open()
        for sql in hql:
            logger.info('Executive sql is:\n%s',sql)
            client.execute(sql)
            # client.fetchAll()
            logger.info('Successful implementation of this Sql')
        transport.close()
    except Thrift.TException, tx:
        logger.error(u'程序执行过程中发生异常, 错误信息如下\n%s',tx.message)
        os.remove(lock_file)
        logger.error(u'程序正在退出. 删除锁文件  %s',lock_file)
        sys.exit(1)

# 执行HQL，有返回结果
def QueryExe(hql, name, dates):
    lock_file = join(lpath, name + '_' + dates + '.lock')
    try:
        transport = TSocket.TSocket(ips, 10001)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ThriftHive.Client(protocol)
        transport.open()
        logger.info('Query sql is:\n%s',hql)
        client.execute(hql)
        query = client.fetchAll()
        logger.info('Query sql result is:\n%s',query)
        transport.close()
        return (query)
    except Thrift.TException, tx:
        logger.error(u'程序执行过程中发生异常, 错误信息如下\n%s',tx.message)
        os.remove(lock_file)
        logger.error(u'程序正在退出. 删除锁文件  %s',lock_file)
        sys.exit(1)

# 日期参数处理
def Pama(dicts, dates):
    today = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    today_iso = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    import datetime
    year = str(dates[0:4])
    month = str(dates[4:6])
    if len(dates) >= 8:
        day = str(dates[6:8])
        hour = str(dates[8:10])
        days = datetime.date(int(dates[0:4]), int(dates[4:6]), int(dates[6:8]))
    else:
        day = '01'
        hour = '00'
        days = datetime.date(int(dates[0:4]), int(dates[4:6]), 1)
    lastday = datetime.date(days.year, days.month, days.day) - datetime.timedelta(1)
    last2day = datetime.date(days.year, days.month, days.day) - datetime.timedelta(2)
    last7day = datetime.date(days.year, days.month, days.day) - datetime.timedelta(7)
    last14day = datetime.date(days.year, days.month, days.day) - datetime.timedelta(14)
    lastmonend = datetime.date(days.year, days.month, 01) - datetime.timedelta(1)
    last2monend = datetime.date(lastmonend.year, lastmonend.month, 01) - datetime.timedelta(1)
    last3monend = datetime.date(last2monend.year, last2monend.month, 01) - datetime.timedelta(1)
    monday = days - datetime.timedelta(days=days.weekday())
    sunday = days + datetime.timedelta(days=6 - days.weekday())
    # 上月同日期
    if days.month == 3:
        if divmod(days.year, 4)[1] == 0:
            if days.day == 31 or days.day == 30:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, 29)
            else:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, days.day)
        else:
            if days.day == 31 or days.day == 30 or days.day == 29:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, 28)
            else:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, days.day)
    else:
        if days.month in (5, 7, 10, 12):
            if days.day == 31:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, 30)
            else:
                lastmon = datetime.date(lastmonend.year, lastmonend.month, days.day)
        else:
            lastmon = datetime.date(lastmonend.year, lastmonend.month, days.day)
    # 上2月同日期
    if days.month == 4:
        if divmod(days.year, 4)[1] == 0:
            if days.day == 30:
                last2mon = datetime.date(last2monend.year, last2monend.month, 29)
            else:
                last2mon = datetime.date(last2monend.year, last2monend.month, days.day)
        else:
            if days.day == 30 or days.day == 29:
                last2mon = datetime.date(last2monend.year, last2monend.month, 28)
            else:
                last2mon = datetime.date(last2monend.year, last2monend.month, days.day)
    else:
        if days.month in (1, 8):
            last2mon = datetime.date(last2monend.year, last2monend.month, 30)
        else:
            last2mon = datetime.date(last2monend.year, last2monend.month, days.day)
    # 上3月同日期
    if days.month == 5:
        if divmod(days.year, 4)[1] == 0:
            if days.day == 31 or days.day == 30:
                last3mon = datetime.date(last3monend.year, last3monend.month, 29)
            else:
                last3mon = datetime.date(last3monend.year, last3monend.month, days.day)
        else:
            if days.day == 31 or days.day == 30 or days.day == 29:
                last3mon = datetime.date(last3monend.year, last3monend.month, 28)
            else:
                last3mon = datetime.date(last3monend.year, last3monend.month, days.day)
    else:
        if days.month in (7, 12):
            last3mon = datetime.date(last3monend.year, last3monend.month, 30)
        else:
            last3mon = datetime.date(last3monend.year, last3monend.month, days.day)
    # 上月的第一天
    lastmon01 = datetime.date(lastmonend.year, lastmonend.month, 1)
    # 本月最后一天
    if month == '12':
        monend = datetime.date(int(year) + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        monend = datetime.date(int(year), int(month) + 1, 1) - datetime.timedelta(days=1)
    # 上年同日期
    lastyearend = datetime.date(days.year, 01, 01) - datetime.timedelta(1)
    if divmod(days.year, 4)[1] == 0:
        if days.month == 2 and days.day == 29:
            lastyear = datetime.date(lastyearend.year, 02, 28)
        else:
            lastyear = datetime.date(lastyearend.year, days.month, days.day)
    else:
        lastyear = datetime.date(lastyearend.year, days.month, days.day)

    dicts['ARG_TODAY'] = today  # 获得yyyymmddhh格式的当前日期
    dicts['ARG_TODAY_ISO'] = today_iso  # 获得yyyy-mm-dd hh格式的当前日期
    dicts['ARG_OPTIME'] = days.strftime('%Y%m%d')  # 获得yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_ISO'] = days  # 获得yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_YEAR'] = year  # 获得yyyy格式的数据日期
    dicts['ARG_OPTIME_MONTH'] = year + month  # 获得yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTMONTH'] = lastmon.strftime('%Y%m')  # 获得yyyymm格式的上月数据日期
    dicts['ARG_OPTIME_LASTMONTH_ISO'] = lastmon.strftime('%Y-%m')  # 获得yyyy-mm格式的上月数据日期
    dicts['ARG_OPTIME_MONTH01'] = year + "-" + '01'  # 获得传入的数据日期的当年第1个月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_MONTH12'] = year + "-" + '12'  # 获得传入的数据日期的当年第12个月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_HOUR'] = year + month + day + hour  # 获得yyyymmddhh格式的数据日期
    dicts['ARG_OPTIME_HOUR_STD'] = hour  # 获得hh格式的数据日期
    dicts['ARG_OPTIME_DAY'] = day  # 获得dd格式的数据日期
    dicts['ARG_OPTIME_THISMON'] = month  # 获得mm格式的数据日期
    dicts['ARG_OPTIME_LASTDAY'] = lastday.strftime('%Y%m%d')  # 获得传入的数据日期的前一天yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTDAY_ISO'] = lastday  # 获得传入的数据日期的前一天yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST2DAY'] = last2day.strftime('%Y%m%d')  # 获得传入的数据日期的两天前yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST2DAY_ISO'] = last2day  # 获得传入的数据日期的两天前yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST7DAY'] = last7day.strftime('%Y%m%d')  # 获得传入的数据日期的七天前yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST7DAY_ISO'] = last7day  # 获得传入的数据日期的七天前yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST14DAY'] = last14day.strftime('%Y%m%d')  # 获得传入的数据日期的14天前yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST14DAY_ISO'] = last14day  # 获得传入的数据日期的14天前yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMON'] = lastmon.strftime('%Y%m%d')  # 获得传入的数据日期的上月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMON_ISO'] = lastmon  # 获得传入的数据日期的上月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST2MON'] = last2mon.strftime('%Y%m%d')  # 获得传入的数据日期的上2月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST2MON_ISO'] = last2mon  # 获得传入的数据日期的上2月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST2MONTH'] = last2mon.strftime('%Y%m')  # 获得传入的数据日期的上2月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LAST2MONTH_ISO'] = last2mon.strftime('%Y-%m')  # 获得传入的数据日期的上2月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_LAST3MON'] = last3mon.strftime('%Y%m%d')  # 获得传入的数据日期的上3月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST3MON_ISO'] = last3mon  # 获得传入的数据日期的上3月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST3MONTH'] = last3mon.strftime('%Y%m')  # 获得传入的数据日期的上3月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LAST3MONTH_ISO'] = last3mon.strftime('%Y-%m')  # 获得传入的数据日期的上3月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_LASTYEAR'] = lastyear.strftime('%Y%m%d')  # 获得传入的数据日期的去年同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTYEAR_ISO'] = lastyear  # 获得传入的数据日期的去年同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTYEARMON'] = lastyear.strftime('%Y%m')  # 获得传入的数据日期的去年同月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTYEARMON_ISO'] = lastyear.strftime('%Y-%m')  # 获得传入的数据日期的去年同月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_YEAR01'] = year + '0101'  # 获得传入的数据日期的当年第一天日期yyyymmdd格式的数据日期 
    dicts['ARG_OPTIME_YEAR01_ISO'] = year + "-" + '01-01'  # 获得传入的数据日期的当年第一天日期yyyy-mm-dd格式的数据日期 
    dicts['ARG_OPTIME_YEAR12'] = year + '1231'  # 获得传入的数据日期的当年最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_YEAR12_ISO'] = year + "-" + '12-31'  # 获得传入的数据日期的当年最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_MON01'] = year + month + '01'  # 获得传入的数据日期的本月第一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MON01_ISO'] = year + "-" + month + "-" + '01'  # 获得传入的数据日期的本月第一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_MONEND'] = monend.strftime('%Y%m%d')  # 获得传入的数据日期的本月最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MONEND_ISO'] = monend  # 获得传入的数据日期的本月最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMON01'] = lastmon01.strftime('%Y%m%d')  # 获得传入的数据日期的上月第一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMON01_ISO'] = lastmon01  # 获得传入的数据日期的上月第一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMONEND'] = lastmonend.strftime('%Y%m%d')  # 获得传入的数据日期的上月最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMONEND_ISO'] = lastmonend  # 获得传入的数据日期的上月最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMON_YYYYMM'] = lastmon01.strftime('%Y%m')  # 获得传入的数据日期的上个月YYYYMM格式
    dicts['ARG_OPTIME_LASTMON_YYYYMM_ISO'] = lastmon01.strftime('%Y-%m')  # 获得传入的数据日期的上个月YYYY-MM格式
    dicts['ARG_OPTIME_MONDAY'] = monday.strftime('%Y%m%d')  # 获得传入的数据日期所在周的周一日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MONDAY_ISO'] = monday  # 获得传入的数据日期所在周的周一日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_SUNDAY'] = sunday.strftime('%Y%m%d')  # 获得传入的数据日期所在周的周日日期yyyymmdd格式的数据日期        
    dicts['ARG_OPTIME_SUNDAY_ISO'] = sunday  # 获得传入的数据日期所在周的周日日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTDAYMON'] = lastday.strftime('%Y%m')  # 获得传入的数据日期的前一天所在月份yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTDAYMON_ISO'] = lastday.strftime('%Y-%m')  # 获得传入的数据日期的前一天所在月份yyyy-mm格式的数据日期
    dicts['ARG_MONTH_DAYS'] = int(lastday.day)  # 获得传入的数据日期的前一天天数

    return dicts

# 写开始锁文件
def Start(name, dates):
    lock_file = join(lpath, name + '_' + dates + '.lock')
    e = os.path.isfile(lock_file)
    if e is False:
        f = open(lock_file, 'w')
        f.close()
        logger.info(u'程序开始执行')
    else:
        logger.error(u'已存在正在运行的其它实例, 该实例主动退出...')
        logger.error(u'如需人工干预请删除锁文件后重新执行. 删除命令: rm -f %s', lock_file)
        sys.exit(1)

# 异常处理
def Except(name, dates, e):
    lock_file = join(lpath, name + '_' + dates + '.lock')
    logger.error(u'程序执行过程中发生异常, 错误信息如下\n%s',e)
    try:
        os.remove(lock_file)
        logger.info(u'程序正在退出. 删除锁文件  %s',lock_file)
    except:
        logger.info(u'程序正在退出. 锁文件 %s不存在无需删除', lock_file)
    sys.exit(1)

# 程序结束，删除锁文件
def End(name, dates):
    lock_file = join(lpath, name + '_' + dates + '.lock')
    try:
        os.remove(lock_file)
        logger.info(u'程序执行成功. 删除锁文件 %s', lock_file)
    except:
        logger.info(u'程序执行成功. 锁文件 %s不存在无需删除', lock_file)
    sys.exit(0)

    
# if __name__ == '__main__':
#    HiveExe(hql)
