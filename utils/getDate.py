# -*- coding:utf-8 -*-
import datetime

d = datetime.datetime.now()

# def day_get(d):
#     oneday = datetime.timedelta(days=1)
#     day = d - oneday
#     date_from = datetime.datetime(day.year, day.month, day.day, 0, 0, 0)
#     date_to = datetime.datetime(day.year, day.month, day.day, 23, 59, 59)
#     # print '---'.join([str(date_from), str(date_to)])

def get_lastweek_from(d):
    """
        返回上个周第一个天
        """
    dayscount = datetime.timedelta(days=d.isoweekday())
    dayto = d - dayscount
    sixdays = datetime.timedelta(days=6)
    dayfrom = dayto - sixdays
    date_from = datetime.datetime(dayfrom.year, dayfrom.month, dayfrom.day, 0, 0, 0)
    return date_from

def get_lastweek_to(d):
    """
        返回上个周最后一天
    """
    dayscount = datetime.timedelta(days=d.isoweekday())
    dayto = d - dayscount
    date_to = datetime.datetime(dayto.year, dayto.month, dayto.day, 23, 59, 59)
    return date_to

def get_lastThursday(d):
    """
      返回上个周四的23:59:59
    """
    lastSunday = get_lastweek_to(d)
    lastThursday = lastSunday - datetime.timedelta(days=2)
    return lastThursday


def get_lastFriday(d):
    """
    """
    lastSunday = get_lastweek_from(d)
    lastFriday = lastSunday + datetime.timedelta(days=4)
    return lastFriday

def get_lastlastFriday(d):
    """
     返回上上个周五的00:00:00
    """
    lastSunday = get_lastweek_from(d)
    lastThursday = lastSunday - datetime.timedelta(days=3)
    return lastThursday

def get_lastmonth_from(d):
    """
    返回上个月第一个天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime.datetime(dayto.year, dayto.month, 1, 0, 0, 0)
    return date_from

def get_lastmonth_to(d):
    """
    返回上个月最后一天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_to = datetime.datetime(dayto.year, dayto.month, dayto.day, 23, 59, 59)
    return date_to

def get_lastquarter_from(d):
    """
    返回上个月第一个天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime.datetime(dayto.year, dayto.month, 1, 0, 0, 0)
    return date_from

# def get_curr_quarter_from(d):
#     """
#     返回上季度最后一天
#     :return
#     date_from: 2016-01-01 00:00:00
#     date_to: 2016-01-31 23:59:59
#     """
#     dayscount = datetime.timedelta(days=d.day)
#     dayto = d - dayscount
#     # lastmonth = get_lastmonth_to(d)
#     if d.month == 1 or d.month == 2 or d.month == 3:
#         return datetime.datetime(dayto.year, 1,  1, 0, 0, 0)
#     elif d.month == 4 or d.month == 5 or d.month == 6:
#         return datetime.datetime(dayto.year, 4,  1, 0, 0, 0)
#     elif d.month == 7 or d.month == 8 or d.month == 9:
#         return datetime.datetime(dayto.year, 7,  1, 0, 0, 0)
#     else:
#         return datetime.datetime(dayto.year, 10,  1, 0, 0, 0)
#
# def get_last_last_quarter_to(d):
#     """
#     返回上上季度最后一天
#     :return
#     date_from: 2016-01-01 00:00:00
#     date_to: 2016-01-31 23:59:59
#     """
#     dayscount = datetime.timedelta(days=d.day)
#     dayto = d - dayscount
#     lastmonth = get_lastmonth_to(d)
#     lastlastmonth = get_lastmonth_to(lastmonth)
#     if lastlastmonth.month == 1 or lastlastmonth.month == 2 or lastlastmonth.month == 3:
#         return datetime.datetime(dayto.year, 3,  dayto.day, 23, 59, 59)
#     elif lastlastmonth.month == 4 or lastlastmonth.month == 5 or lastlastmonth.month == 6:
#         return datetime.datetime(dayto.year, 6,  dayto.day, 23, 59, 59)
#     elif lastlastmonth.month == 7 or lastlastmonth.month == 8 or lastlastmonth.month == 9:
#         return datetime.datetime(dayto.year, 9,  dayto.day, 23, 59, 59)
#     else:
#         return datetime.datetime(dayto.year, 12, dayto.day, 23, 59, 59)