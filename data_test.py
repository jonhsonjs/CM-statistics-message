# -*- coding:utf-8 -*-
import sys
from common.timeseries import TimeSeriesQuery
from cm_api.endpoints.timeseries import *
from utils.utc import utc2local
import numpy as np
import pandas as pd


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


def getImpalaAttrs(query,attrs,from_time,to_time):
  line = []
  vals = []
  user=[]
  attrs = ['user','database','statement','hdfs_bytes_read','memory_accrual','memory_aggregate_peak']

  responseList = do_query(query, from_time, to_time)
  for response in responseList:
    if response.timeSeries:
      for ts in response.timeSeries:
        metadata = ts.metadata
        for data in ts.data:
          if metadata.attributes:
            line.append(utc2local(data.timestamp).strftime("%m-%d %H:%M:%S"))
            for attr in attrs:
              if metadata.attributes.has_key(attr):
                attrVal = metadata.attributes[attr]
                line.append(attr + ':' + attrVal)
              else:
                line.append(attr + ':' + 'N/A')
            vals.append(line)
  return vals


def main(argv):
  now = datetime.datetime.now()
  one_Min_Ago = now - datetime.timedelta(minutes=5)
  one_Hour_Ago = now - datetime.timedelta(hours=1)  # 前一小时
  one_day_Ago = now -datetime.timedelta(days=1)




  # attrsStatsMissing = ['user', 'database', 'statement']
  # attrsHdfsBytesRead = ['user', 'database', 'statement','hdfs_bytes_read']
  # attrsMemoryAggregatePeak = ['user', 'database', 'statement', 'memory_aggregate_peak']
  #
  # statsMissing = getImpalaAttrs(
  #   "select query_duration from IMPALA_QUERIES where serviceName=impala and stats_missing=true",
  #   attrsStatsMissing,one_Min_Ago, now)
  # print "\nstatsMissing-------------\n"
  # print statsMissing
  #
  # hdfsBytesRead = getImpalaAttrs(
  #   "select query_duration from IMPALA_QUERIES where service_name = impala and  hdfs_bytes_read >= 2.43E9",
  #   attrsHdfsBytesRead, one_Min_Ago, now)
  # print "\nhdfsBytesRead-------------\n"
  # print hdfsBytesRead
  #
  # memoryAggregatePeak = getImpalaAttrs("select query_duration from IMPALA_QUERIES where service_name = impala and memory_aggregate_peak >= 6.0E9",attrsMemoryAggregatePeak, one_Min_Ago, now)
  # print "\nmemoryAggregatePeak-------------\n"
  # print memoryAggregatePeak


if __name__ == '__main__':
  sys.exit(main(sys.argv))
