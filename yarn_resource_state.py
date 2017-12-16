# -*- coding:utf-8 -*-

from common.timeseries import TimeSeriesQuery
from utils.date_util import get_n_day_ago_from
import os
from utils.getConfig import get_conf
from utils.sendmail import sendmail
import sys


reload(sys)
sys.setdefaultencoding('utf-8')
FROM = get_conf("mail", "FROM")


class Granularity:
    RAW = 1  # every 1 Seconds
    TEN_MINUTES = 10  # every 10 minutes(100Seconds）
    MINUTE = 1  # everty 1 minute
    HOURLY = 3600  # every 600 Seconds
    SIX_HOURS = 3600  # every 6 hours(3600Seconds)
    DAILY = 14400  # every 14400 Seconds
    WEEKLY = 100800  # every 100800 Seconds


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


def getUsedrate(query, from_time, to_time, sqParam1, sqParam2):
    list_info = []
    responseList = do_query_rollup(query, from_time, to_time, 'HOURLY', True)
    entityNames = []
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                metadata = ts.metadata
                entityName = metadata.attributes['entityName']
                if str(entityName).endswith("group"):
                    entityNames.append(entityName)
    for entityName in entityNames:
        used_rate = []
        entity_info = {}
        tempqy1 = "SELECT " + sqParam1 + " WHERE entityName =\"" + entityName + "\"AND category = YARN_POOL"
        tempqy2 = "SELECT " + sqParam2 + " WHERE entityName =\"" + entityName + "\"AND category = YARN_POOL"
        responseList = do_query_rollup(tempqy1, from_time, to_time, 'HOURLY', True)
        for response in responseList:
            if response.timeSeries:
                for ts in response.timeSeries:
                    used_value = []
                    for Value in ts.data:
                        if Value.value != 0:
                            resultValue = Value.value
                            if sqParam1 == "fair_share_mb":
                                resultValue = '%.1f' % ((float)(resultValue) / 1024)
                            used_value.append(resultValue)
                        else:
                            used_value.append(0)
        responseList = do_query_rollup(tempqy2, from_time, to_time, 'HOURLY', True)
        for response in responseList:
            if response.timeSeries:
                for ts in response.timeSeries:
                    max_value = []
                    for Value in ts.data:
                        if Value.value != 0:
                            resultValue = Value.value
                            if sqParam2 == "max_share_mb":
                                resultValue = '%.1f' % ((float)(resultValue) / 1024)
                            max_value.append(resultValue)
                        else:
                            max_value.append(0)
        for i in range(len(used_value)):
            if max_value[i] != 0:
                used_gate = '%.2f' % ((float)(used_value[i]) / (float)(max_value[i]))
                used_gate = int((float)(used_gate) * 100)
                if used_gate == 0:
                    used_rate.append('N/A')
                else:
                    used_rate.append(str(used_gate))
        entity_info[entityName] = used_rate
        list_info.append(entity_info)
    return list_info


def generateTable(list_info, attrs):
    html = []
    html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td style='width:300px;'>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")
    for info in list_info:
        html.append("<tr>")
        for key in info:
            html.append("<td>")
            html.append(key)
            html.append("</td>")
            rate_value = info[key]
            for i in range(len(rate_value)-1):
                html.append("<td>")
                html.append(rate_value[i])
                html.append("</td>")
        html.append("</tr>")
    html.append("</table>")
    report = ''.join(html)
    return report


if __name__ == '__main__':
    attrs1 = ['entityName', '21:00', '22:00', '23:00', '00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00',
             '07:00', '08:00']
    attrs2 = ['entityName', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00',
              '19:00', '20:00']
    today_floor_hour = get_n_day_ago_from(n=0)
    date_yesterday = get_n_day_ago_from(n=1)
    today_nine = today_floor_hour.replace(hours=9).floor('hour')
    yesterday_nine = today_floor_hour.replace(hours=-3).floor('hour')
    yesterday_nine_tonight = today_floor_hour.replace(hours=-15).floor('hour')
    list_info_night = getUsedrate("SELECT max_share_vcores WHERE category = YARN_POOL", yesterday_nine, today_nine, "fair_share_vcores",
                       "max_share_vcores")
    report_voce_yesterday_night = generateTable(list_info_night, attrs1)
    list_info_day = getUsedrate("SELECT max_share_vcores WHERE category = YARN_POOL", yesterday_nine_tonight, yesterday_nine,
                                "fair_share_vcores",
                                "max_share_vcores")
    report_voce_yesterday_day = generateTable(list_info_day, attrs2)
    cpu_info_night = getUsedrate("SELECT max_share_mb WHERE category = YARN_POOL", yesterday_nine, today_nine, "fair_share_mb",
                      "max_share_mb")
    report_cpu_yesterday_night = generateTable(cpu_info_night, attrs1)
    cpu_info_day = getUsedrate("SELECT max_share_mb WHERE category = YARN_POOL", yesterday_nine_tonight, yesterday_nine,
                                 "fair_share_mb",
                                 "max_share_mb")
    report_cpu_yesterday_day = generateTable(cpu_info_day, attrs2)
    pwd = os.getcwd()
    picList = []
    SUBJECT = "资源池统计情况" + date_yesterday.strftime('%Y-%m-%d')
    mail_msg = "<h1>成都综合生产集群资源池报告</h1>"
    mail_msg += "<br>白天Vcore使用率统计</br>"
    mail_msg += report_voce_yesterday_day
    mail_msg += "<br>晚上Vcore使用率统计</br>"
    mail_msg += report_voce_yesterday_night
    mail_msg += "<br>白天CPU使用率统计</br>"
    mail_msg += report_cpu_yesterday_day
    mail_msg += "<br>晚上CPU使用率统计</br>"
    mail_msg += report_cpu_yesterday_night
    TO = ['jhonshonjs@163.com', 'yuanbowen1@wanda.cn', 'ourui@wanda.cn', 'wanghuan70@wanda.cn']
    ACC = []
    sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)


