# -*- coding:utf-8 -*-
import sys
from common.timeseries import TimeSeriesQuery
from cm_api.endpoints.timeseries import *
from utils.utc import utc2local


# 数据粒度，用于图表时间显示的单位（例如天、小时等）的判断
class Granularity:
    RAW = 1  # every 1 Seconds
    TEN_MINUTES = 10  # every 10 minutes(100Seconds）
    MINUTE = 1  # everty 1 minute
    HOURLY = 600  # every 600 Seconds
    SIX_HOURS = 3600  # every 6 hours(3600Seconds)
    DAILY = 14400  # every 14400 Seconds
    WEEKLY = 100800  # every 100800 Seconds
    # AUTO = -1

def do_query(query, from_time, to_time):
  responseList = []
  tsquery = TimeSeriesQuery()
  for response in tsquery.query(query, from_time, to_time):
      responseList.append(response)
  return responseList


def do_query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup):
    responseList = []
    tsquery = TimeSeriesQuery()
    for response in tsquery.query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup):
        responseList.append(response)
    return responseList


def get_cpu_used_info(query,from_time,to_time):
  line = []
  responseList = do_query_rollup(query, from_time, to_time, 'RAW', False)
  for response in responseList:
    if response.timeSeries:
      for ts in response.timeSeries:
        metadata = ts.metadata
        for data in ts.data:
            time_value = {}
            max_values = []
            x_time = utc2local(data.timestamp).strftime("%m-%d %H:%M")
            if data.aggregateStatistics:
                time_value[x_time] = data.aggregateStatistics.max
                line.append(time_value)
  return line


def main(argv):
    now = datetime.datetime.now()
    one_hour_ago = now -datetime.timedelta(hours=1)
    two_hour_ago = now - datetime.timedelta(hours=24)
    one_day_Ago = now - datetime.timedelta(days=1)
    one = []
    data = get_cpu_used_info("SELECT cpu_percent_across_hosts WHERE entityName = '1' AND category = CLUSTER",
                             one_hour_ago, now)
    with open('cpuData.csv', 'a') as c:
        for dt in data:
            for i in dt.keys():
                c.write(i + ",")
                c.write(str(dt[i]))
                c.write("\n")
if __name__ == '__main__':
    sys.exit(main(sys.argv))