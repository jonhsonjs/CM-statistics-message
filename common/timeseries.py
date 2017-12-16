#!/usr/bin/env python
from cm_api.api_client import ApiResource
from utils.getConfig import get_conf
from cm_api.endpoints import timeseries

# CM_HOST = get_conf("clouderaTest", "CM_HOST")
# CM_USER = get_conf("clouderaTest", "CM_USER")
# CM_PASSWD = get_conf("clouderaTest", "CM_PASSWD")
# VERSION = get_conf("clouderaTest","VERSION")

CM_HOST = get_conf("clouderaProd", "CM_HOST")
CM_USER = get_conf("clouderaProd", "CM_USER")
CM_PASSWD = get_conf("clouderaProd", "CM_PASSWD")
VERSION = get_conf("clouderaProd","VERSION")

class TimeSeriesQuery(object):
  def __init__(self):
    self._api = ApiResource(CM_HOST, username=CM_USER, password=CM_PASSWD, use_tls=False,version=VERSION)

  def query(self, query, from_time, to_time):
    return self._api.query_timeseries(query, from_time, to_time)

  def query_rollup(self, query, from_time, to_time,desired_rollup=None, must_use_desired_rollup=None):
    return timeseries.query_timeseries(self._api,query,from_time,to_time,desired_rollup, must_use_desired_rollup)



