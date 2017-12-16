# -*- coding:utf-8 -*-

import sys

import arrow

from collections import namedtuple


def get_now():
    return arrow.now()


def get_one_day_ago_from(day=arrow.now()):
    return day.replace(days=-1).floor('day')


def get_n_hour_ago_from(day=arrow.now(), n=10):
    return day.replace(hours=-n).floor('hour')


def get_n_day_ago_from(day=arrow.now(), n=4):
    return day.replace(days=-n).floor('day')


def get_last_n_week_from(day=arrow.now(), n=5):
    """
        返回上个周第一个天
    """
    return day.replace(weeks=-n).floor('week')


def get_last_week_from(day=arrow.now()):
    """
        返回上个周第一个天
    """
    return day.replace(weeks=-1).floor('week')


def get_last_week_to(day=arrow.now()):
    """
        返回上个周最后一天
    """
    return day.replace(weeks=-1).ceil('week')


def get_last_thursday(day=arrow.now()):
    """
      返回上个周四的23:59:59
    """
    last_sunday = get_last_week_to(day)
    last_thursday = last_sunday.replace(days=-3)
    return last_thursday


def get_last_friday(day=arrow.now()):
    """
    """
    last_sunday = get_last_week_to(day)
    last_friday = last_sunday.replace(days=-2).floor('day')
    return last_friday


def get_last_last_friday(day=arrow.now()):
    """
     返回上上个周五的00:00:00
    """
    return get_last_friday(day).replace(weeks=-1)


def get_last_n_month_from(day=arrow.now(), n=5):
    """
    返回上个月第一个天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    return day.replace(months=-n).floor('month')


def get_last_month_from(day=arrow.now()):
    """
    返回上个月第一个天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    return day.replace(months=-1).floor('month')


def get_last_month_to(day=arrow.now()):
    """
    返回上个月最后一天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    return day.replace(months=-1).ceil('month')


def get_quarter_range(day=arrow.now()):
    """
    返回上个月第一个天
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    return day.span('quarter')


def get_last_n_quarter_from(day=arrow.now(), n=5,  format=u'YYYY-MM'):
    """
    """
    days = []
    for i in range(n):
        quarter_from, quarter_to = get_quarter_range(day)
        days.append(quarter_to.format(format))
        day = quarter_from.replace(days=-1)
    return days, quarter_from


def zone_conversion(timestamp=u'1970-01-01', zone=u'Asia/Shanghai', format=u'YYYY-MM-DD HH:MM'):
    return arrow.get(timestamp).to(zone).format(format)


def zone_conversion_date(timestamp=u'1970-01-01', zone=u'Asia/Shanghai'):
    return arrow.get(timestamp).to(zone)


def zone_conversion_date_format(timestamp, zone=u'Asia/Shanghai', format=u'MM-DD HH:mm:ss'):
    return arrow.get(timestamp).to(zone).format(format)


def round_milli_time(start_time, end_time):
    return ((arrow.get(end_time) - arrow.get(start_time)).seconds)*1000


class Quarter(namedtuple('Quarter', 'year quarter')):
    __slots__ = ()

    @classmethod
    def from_date(cls, date):
        """Create Quarter from datetime.date instance."""
        return cls(date.year, (date.month - 1) // 3 + 1)

    @classmethod
    def from_string(cls, text):
        date = zone_conversion_date(timestamp=text)
        return cls.from_date(date)

    def increment(self):
        """Return the next quarter."""
        if self.quarter < 4:
            return self.__class__(self.year, self.quarter + 1)
        else:
            assert self.quarter == 4
            return self.__class__(self.year + 1, 1)

    def __str__(self):
        """Convert to "NQM" text representation."""
        return "{year}-Q{quarter}".format(year=self.year, quarter=self.quarter)


def main(argv):
    # Do work
    t = get_n_hour_ago_from()
    print t

    return 0


#
# The "main" entry
#
if __name__ == '__main__':
    sys.exit(main(sys.argv))
