# -*- coding:utf-8 -*-
import os
import sys
from common.timeseries import TimeSeriesQuery
from cm_api.endpoints.timeseries import *
from utils.utc import utc2local
from utils.getConfig import get_conf
from utils.sendmail import sendmail
import json
import urllib2
import string
from common.api_client import ApiClient
from cm_api.endpoints.services import  get_service
from utils.date_util import round_milli_time
# 导入一个函数将数据转为字典

reload(sys)
sys.setdefaultencoding('utf-8')
#邮箱配置
FROM = get_conf("mail", "FROM")


def do_query(query, from_time, to_time):
    # return api_ts list
  responseList = []
  tsquery = TimeSeriesQuery()
  for response in tsquery.query(query, from_time, to_time):
      responseList.append(response)
  return responseList


# define function:getAllUserInfo to get all unique users
def getAllUserInfo(query, attrs, from_time, to_time):
    '''
    return a list of dict for each user info 返回一个由字典组成的列表，字典里面由各个属性加上对应的值的列表
    '''
    # generate attrs dict for each user
    user_info = []
    get_alluser_info = []
    responseList = do_query(query, from_time, to_time)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                metadata = ts.metadata
                user_dict = {}
                if metadata.attributes:
                    for attr in attrs:
                        if attr in metadata.attributes:
                            attrVal = metadata.attributes[attr]
                            #将jobId写到到文件entityName文件中
                            if ('hdfs_bytes_read' == attr):
                                if ((float)(attrVal) > 1024 * 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 1024 / 1024 / 1024)) + "G"
                                elif ((float)(attrVal) > 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 1024 / 1024)) + "M"
                                elif ((float)(attrVal) > 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 1024)) + "K"
                            if (('memory_accrual' == attr) or ('memory_aggregate_peak' == attr)):
                                if ((float)(attrVal) > 8 * 1024 * 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024 / 1024)) + "G"
                                elif ((float)(attrVal) > 8 * 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024)) + "M"
                                elif ((float)(attrVal) > 8 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024)) + "K"
                            if ('mb_millis' == attr):
                                if ((float)(attrVal) > 8 * 1024 * 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024 / 1024)) + "G"
                                elif ((float)(attrVal) > 8 * 1024 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024)) + "M"
                                elif ((float)(attrVal) > 8 * 1024):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024)) + "K"
                            if ('application_duration' == attr) :
                                if (int((float)(attrVal)) > 60 * 60 * 1000):
                                    attrVal = ('%.2f' % ((float)(attrVal) / 60 / 60 / 1000)) + "h"  # 小时
                                elif (int((float)(attrVal)) > 60 * 1000):
                                    attrVal = (str)(int((float)(attrVal)) / 60 / 1000) + "m"  # 分
                                elif (int((float)(attrVal)) > 1000):
                                    attrVal = (str)(int((float)(attrVal)) / 1000) + "s"  # 秒
                            if ('query_duration' == attr) :
                                if (int((float)(attrVal)) > 60 * 1000):
                                    attrVal = (str)(int((float)(attrVal)) / 60 / 1000) + "m"  # 分
                                elif (int((float)(attrVal)) > 1000):
                                    attrVal = (str)(int((float)(attrVal)) / 1000) + "s"  # 秒
                            user_dict[attr] = attrVal
                        else:
                            user_dict[attr] = 'N/A'
                for data in ts.data:
                    user_dict['time'] = utc2local(data.timestamp).strftime("%m-%d %H:%M:%S")
                    user_info.append(user_dict)
    for info in user_info:
        if info. has_key('user'):
            all_user_info = {}
            username = info['user']
            all_user_info[username] = info
            get_alluser_info.append(all_user_info)
    return get_alluser_info

class HiveInfo(ApiClient):
    """

    """
    def get_hive_user_demo(self, from_time, to_time, condition):
        filter_str = "hive_query_id RLIKE \".*\" and "+ condition
        return get_service(self._api, cluster_name="cluster", name="yarn").get_yarn_applications(start_time=from_time, end_time=to_time, filter_str=filter_str)


def do_get_hive_user_demo(from_time, to_time,condition):
    attrs = [ 'time','user', 'application_duration', 'category', 'service_name', 'name', 'entityName']

    hive_info = HiveInfo()
    top_users = hive_info.get_hive_user_demo(from_time=from_time, to_time=to_time, condition=condition)
    massage_dfs = []
    list_users = []
    if top_users.applications:
        for i in top_users.applications:
            user_info = {}
            line = {}
            list_users.append(i.user)
            line['category'] = "YARN_APPLICATION"
            line['service_name'] = "yarn"
            line['pool'] = i.pool
            line['user'] = i.user
            # line['cpu_milliseconds'] = i.attributes['cpu_milliseconds']
            line['name'] = i.attributes['hive_query_string']
            line['entityName'] = i.applicationId
            line['time'] = utc2local(i.startTime).strftime("%m-%d %H:%M:%S")
            attr_val = round_milli_time(i.startTime, i.endTime)
            line['application_duration1'] = attr_val
            if int(float(attr_val)) > 60 * 60 * 1000:
                attr_val = ('%.2f' % (float(attr_val) / 60 / 60 / 1000)) + "h"
            elif int(float(attr_val)) > 60 * 1000:
                attr_val = str(int(float(attr_val)) / 60 / 1000) + "m"  # 分
            elif int(float(attr_val)) > 1000:
                attr_val = str(int(float(attr_val)) / 1000) + "s"  # 秒
            line['application_duration'] = attr_val
            user_info[i.user] = line
            massage_dfs.append(user_info)
    h = []
    report = []
    for i in range(len(massage_dfs)):
        html = []
        html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
        for attr in attrs:
            html.append("<td>")
            html.append(attr)
            html.append("</td>")
        html.append("</tr>")
        item = massage_dfs[i]
        user_info_dict = item[list_users[i]]
        html.append("<tr>")
        line = []
        for attr in attrs:
            html.append("<td>")
            html.append(user_info_dict[attr])
            html.append("</td>")
        html.append("</tr>")
        x = ''.join(line)
        html.append(x)
        html.append("</table>")
        h.append(html)
        re = {list_users[i]: h[i]}
        report.append(re)
    return (list_users, report)


# define function:generateEmailMsg to generate email msg
def generateEmailMsg(all_user_info,attrs):
    '''
    generate email msg context 一个用户对应自己的信息，相当于一个用户一张表格
    '''
    h = []
    report = []
    list_users = []
    for info in all_user_info:
        for key in info:
            list_users.append(key)
    for i in range(len(list_users)):
        html = []
        html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
        for attr in attrs:
            html.append("<td>")
            html.append(attr)
            html.append("</td>")
        html.append("</tr>")
        line = []
        for attr in attrs:
            line.append("<td>")
            info_dict = all_user_info[i]
            for key in info_dict:
                user_info = info_dict[key]
                line.append(user_info[attr])
            line.append("</td>")
        line.append("</tr>")
        x = ''.join(line)
        html.append(x)
        html.append("</table>")
        h.append(html)
        re = {list_users[i]: h[i]}
        report.append(re)
    return report


# 判断user是否是一个组用户，然后拼接邮件收件人
def ifgroup(user):
    TO = []
    # 此处为我们自己写的一个用户接口#此处为我们自己写的一个用户接口
    url = "xxx"
    req = urllib2.urlopen(url)
    response = req.read()
    text = json.loads(response)
    json_data = text['data']
    if json_data['count'] == 0:
        TO = [user+"@xx.cn"]
    else:
        group = json_data['results']
        for group_user in group:
            TO.append(group_user['email'])
    return TO


def main(argv):
    now = datetime.datetime.now()
    ten_Min_Ago = now - datetime.timedelta(minutes=10)  # 前10分钟
    one_Hour_Ago = now - datetime.timedelta(hours=2)  # 前1小时

    attrs = ['time','user', 'database', 'query_duration', 'statement','stats_missing', 'hdfs_bytes_read', 'memory_accrual', 'memory_aggregate_peak']

    statsMissing_info = getAllUserInfo(
      "select query_duration from IMPALA_QUERIES where serviceName = impala and stats_missing=true and query_duration>=600000.0",attrs,ten_Min_Ago, now)
    memoryAggregatePeak_info = getAllUserInfo("select query_duration from IMPALA_QUERIES where service_name = impala and memory_aggregate_peak >= 1.7E11",attrs, ten_Min_Ago, now)

    # get hiveMemory users and hiveMemory info dict
    hiveMemory_tuple = do_get_hive_user_demo(ten_Min_Ago, now, 'mb_millis >=1.7E12')
    hiveMemory_users = hiveMemory_tuple[0]
    hiveMemory_report_list = hiveMemory_tuple[1]

    # get hiveMap users and hiveMap info dict
    hiveMap_tuple = do_get_hive_user_demo(ten_Min_Ago, now, 'maps_total >= 3000')
    hiveMap_users = hiveMap_tuple[0]
    hiveMap_report_list = hiveMap_tuple[1]

    # get hiveReduce users and hiveReduce info dict
    hiveReduce_tuple =do_get_hive_user_demo(ten_Min_Ago,now,'reduces_total >= 300')
    hiveReduce_users = hiveReduce_tuple[0]
    hiveReduce_report_list = hiveReduce_tuple[1]

    # get statsMissing user
    statsMissing_users = []
    for info in statsMissing_info:
        for key in info:
            statsMissing_users.append(key)

    # get memoryAggregatePeak user
    memoryAggregatePeak_users = []
    for info in memoryAggregatePeak_info:
        for key in info:
            memoryAggregatePeak_users.append(key)

    # 统计impala statsMissing
    if statsMissing_users:
        msg = generateEmailMsg(statsMissing_info, attrs)
        for i in range(len(statsMissing_users)):
            user = statsMissing_users[i]
            query_duration = statsMissing_info[i][user]['query_duration']
            TO = ifgroup(user)

            pwd = os.getcwd()
            picList =[]
            if int(query_duration[0]) >= 30:
                print "该邮件已经发送"
            else:
                mail_msg = "<br>以下查询缺少统计信息，缺乏统计数据会导致会选择错误的执行策略，导致任务变慢，建议运行compute stats database.table命令,请关注：</br>"
                Msg = msg[i]
                mail_msg += ''.join(Msg[statsMissing_users[i]])
                SUBJECT = "Impala compute stats统计信息缺失告警" + datetime.datetime.now().strftime('%Y-%m-%d')
                TO=['xxx']
                ACC = []
                sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

    # 统计impala运行内存较大的任务
    if memoryAggregatePeak_users:
        msg = generateEmailMsg(memoryAggregatePeak_info, attrs)
        for i in range(len(memoryAggregatePeak_users)):
            user = memoryAggregatePeak_users[i]
            TO = ifgroup(user)

            pwd = os.getcwd()
            picList = []
            mail_msg = "<br>以下查询内存消耗超过20G，请关注：</br>"

            Msg = msg[i]
            mail_msg += ''.join(Msg[memoryAggregatePeak_users[i]])
            SUBJECT = "Imapla消耗内存较大告警" + datetime.datetime.now().strftime('%Y-%m-%d')
            TO = ['xxx']
            ACC = []
            sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

    # 统计Hive中运行内存较大的任务
    if hiveMemory_users:
        for i in range(len(hiveMemory_users)):
            user = hiveMemory_users[i]
            TO = ifgroup(user)

            pwd = os.getcwd()
            picList = []
            mail_msg = "<br>以下查询内存消耗超过200G，请关注：</br>"

            Msg = hiveMemory_report_list[i]
            mail_msg += ''.join(Msg[hiveMemory_users[i]])
            SUBJECT = "Hive消耗内存较大告警" + datetime.datetime.now().strftime('%Y-%m-%d')
            TO = ['xxx']
            ACC = []
            sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

    # 统计Hive中消耗map数大于3000个的任务
    if hiveMap_users:
        for i in range(len(hiveMap_users)):
            user = hiveMap_users[i]
            TO = ifgroup(user)

            pwd = os.getcwd()
            picList = []
            mail_msg = "<br>以下查询使用map数量大于3000个，请调节相关参数，合理控制Map个数，查询详情如下：</br>"
            Msg = hiveMap_report_list[i]
            mail_msg += ''.join(Msg[hiveMap_users[i]])
            SUBJECT = "Hive任务中map数较大告警" + datetime.datetime.now().strftime('%Y-%m-%d')
            TO = ['xxx']
            ACC = []
            sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

    # 统计Hive中消耗reduce数超过300个的任务
    if hiveReduce_users:
        for i in range(len(hiveReduce_users)):
            user = hiveReduce_users[i]
            TO = ifgroup(user)

            pwd = os.getcwd()
            picList = []
            mail_msg = "<br>以下查询使用reduce数量大于300个，请关注：</br>"
            Msg = hiveReduce_report_list[i]
            mail_msg += ''.join(Msg[hiveReduce_users[i]])
            SUBJECT = "Hive任务中reduce数较大告警" + datetime.datetime.now().strftime('%Y-%m-%d')
            TO = ['xxx']
            ACC = []
            sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

if __name__ == '__main__':
  sys.exit(main(sys.argv))