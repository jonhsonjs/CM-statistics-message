# -*- coding:utf-8 -*-
import datetime
import os
import sys
import string
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as dt
import pylab as pl
import pygal
import heapq
import ssh
from impala.dbapi import connect
from common.timeseries import TimeSeriesQuery
from cm_api.endpoints.timeseries import *
from utils.getConfig import get_conf
from utils.sendmail import sendmail
from utils.utc import utc2local
import utils.getDate
from utils.date_util import round_milli_time, zone_conversion
from utils.line_model import get_linear_model
from common.api_client import ApiClient
from cm_api.endpoints.services import get_service

reload(sys)
sys.setdefaultencoding('utf-8')

# 解决Matplotlib中文问题
pl.mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
pl.mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-

# 邮箱配置
FROM = get_conf("mail", "FROM")
# TO = string.splitfields(get_conf("mail", "TO"), ',')
SUBJECT = get_conf("mail", "SUBJECT") + datetime.datetime.now().strftime('%Y-%m-%d')
# ACC = []

CAPTION = get_conf("pic", "caption")
CAPTIONCPU = get_conf("pic", "captionCPU")
CAPTIONMEM = get_conf("pic", "captionMEM")
CAPTIONNET = get_conf("pic", "captionNET")
filenameCPU = get_conf("pic", "filenameCPU")
filenameMEM = get_conf("pic", "filenameMEM")
filenameNET = get_conf("pic", "filenameNET")
SUFIX = get_conf("pic", "SUFIX")


# WIDTH = get_conf("pic","WIDTH")
# HEIGHT = get_conf("pic","HEIGHT")

# 数据粒度，用于图表时间显示的单位（例如天、小时等）的判断
class Granularity:
    RAW = 1  # every 1 Seconds
    TEN_MINUTES = 10  # every 10 minutes(100Seconds）
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


# 获取日报
def getReportChart(query, from_time, to_time, caption, filename, granularity, desired_rollup, must_use_desired_rollup):
    # if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
    #   caption = caption + ' (' + from_time.strftime('%Y%m%d') + ')'  # 日报标题
    # else:
    #   caption = caption + ' (' + from_time.strftime('%Y%m%d') + '--' + to_time.strftime('%Y%m%d') + ')'  # 周报标题

    fileName = filename + ".png"
    plt.style.use('ggplot')
    plt.figure(figsize=(14, 4))
    maxY = 0  # Y轴最大值
    type = 'SAMPLE'  # 数据类型，用于判断是否是计算变量还是原始变量
    responseList = do_query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                x = []
                yMax = []
                yMean = []
                metadata = ts.metadata
                unit = metadata.unitNumerators[0].encode("utf-8")
                y_title = unit
                for data in ts.data:
                    x_label = utc2local(data.timestamp).strftime("%Y-%m-%d %H:%M:%S")
                    x_time = datetime.datetime.strptime(x_label, "%Y-%m-%d %H:%M:%S")
                    x.append(x_time)
                    if (None != data.aggregateStatistics):
                        yMax.append(data.aggregateStatistics.max)
                    yMean.append(data.value)
                    type = data.type
                legend = metadata.entityName
                if (legend == u'综合生产集群'):
                    code = '-'
                elif (legend == u'公共服务集群'):
                    code = ':'
                elif (legend == u'准实时生产集群'):
                    code = '--'
                else:
                    code = '-.'
                if ([] != yMax):
                    labelMax = legend + "Max"
                    # print labelMax
                    pl.plot_date(x, yMax, label=labelMax, linestyle=code, linewidth=1)
                labelAvg = legend + "Avg"
                pl.plot_date(x, yMean, label=labelAvg, linestyle=code, linewidth=1)
                #   line_chart1.add(code + "--Max", yMax,stroke_style={'width': 2, 'dasharray': '1, 3', 'linecap': 'round', 'linejoin': 'round'})
                # line_chart1.add(code + "--Avg", yMean)
                # pl.plot_date(x, y, label=legend, linestyle=code, linewidth=1)
                if ([] != yMax):
                    maxY = max(maxY, max(yMax))
                maxY = max(maxY, max(yMean))

    # X轴时间显示格式
    ax = pl.gca()
    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
        xfmt = dt.DateFormatter('%H:%M')
    elif (Granularity.HOURLY == granularity):
        xfmt = dt.DateFormatter('%m-%d %H:%M')
    else:
        xfmt = dt.DateFormatter('%y-%m-%d')
    ax.xaxis.set_major_formatter(xfmt)
    pl.gcf().autofmt_xdate()
    # Y轴单位，例如50%，100%
    if ('SAMPLE' == type):
        # 如果是原始变量，直接根据单位设置Y轴单位后缀
        if ('percent' == unit):
            maxYTick = maxY
            suffix = '%'  # 单位后缀
        elif ('bytes' == unit):
            if (maxY > 1024 * 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024 / 1024
                suffix = 'T'
            if (maxY > 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024
                suffix = 'G'
            elif (maxY > 1024 * 1024):
                maxYTick = maxY / 1024 / 1024
                suffix = 'M'
            elif (maxY > 1024):
                maxYTick = maxY / 1024
                suffix = 'K'  # 网络流量单位转换
            else:
                maxYTick = maxY
                suffix = 'b'
    else:
        # 如果是计算变量，默认设置
        maxYTick = maxY
        suffix = '%'
    y_ticks = [0, 0.5 * maxY, maxY]
    y_tickslabels = ['0', "%.1f" % (0.5 * maxYTick) + suffix, "%.1f" % (maxYTick) + suffix]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_tickslabels)
    pl.title(caption)
    pl.legend(loc='best', fontsize=7)
    pl.grid(True)
    pl.ylabel(y_title)
    pl.savefig(fileName)
    pl.cla()
    return fileName


# 查询hdfs上的文件所有者以及对应的文件大小
def queryAddFiles(command):
    attrs = ['user', 'size']
    user_dict = dict((attr, []) for attr in attrs)
    client = ssh.SSHClient()
    client.set_missing_host_key_policy(ssh.AutoAddPolicy())
    client.connect('xxx', 22, 'xxx', 'xxx')
    stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read()
    data_list = []
    data_list.extend(out.split('{')[1:])
    data = []
    for each in data_list:
        each = "{" + each
        data.append(each)
    data = ''.join(data)
    text = json.loads(data)
    list_data = text['items']
    user_info = {}
    for user_msg in list_data:
        for attr in attrs:
            if attr == 'user':
                user_dict[attr].append(user_msg['user'])
            if attr == 'size':
                user_dict[attr].append(user_msg['rawSize'])
        user_info[user_msg['user']] = user_msg['rawSize']
    return (user_dict, user_info)


# 查询文件的目录结构,组装成字典方便后面对比
def quer_contents(command):
    conn = connect(host='xxx', port=21050, user='xxx', password='xxx', auth_mechanism="PLAIN")
    cur = conn.cursor()
    cur.execute("refresh idc_infrastructure_db.hdfs_meta_dir_all_daily")
    cur.execute(command)
    rows = cur.fetchall()
    attrs = ['joinedpath', 'size', 'date']
    content_dict = dict((attr, []) for attr in attrs)
    content_info = {}
    for row in rows:
        content_dict['joinedpath'].append(row[0])
        content_dict['size'].append(row[1])
        content_dict['date'].append(row[2])
        content_info[row[0]] = row[1]
    return (content_dict, content_info)


# 从impala中获取hive中的具体表信息以及数据库信息
def query_hive_table(command):
    conn = connect(host='xxx', port=21050, user='xxx', password='xxx', auth_mechanism="PLAIN")
    cur = conn.cursor()
    cur.execute("refresh idc_infrastructure_db.hive_table_info_all_daily")
    cur.execute(command)
    rows = cur.fetchall()
    hive_db_name_list = []
    hive_table_info = []
    for row in rows:
        hvie_table_dict = {}
        hive_db_name_list.append(row[0])
        hvie_table_dict[row[1]] = row
        hive_table_info.append(hvie_table_dict)
    return (hive_db_name_list, hive_table_info)


# 拼装Hive中数据库以及表的增量数据
def do_get_info_adding(info_today, info_yesterday):
    adding_info = {}
    add_db_list = []
    add_table_list = []
    db_list_today = list(set(info_today[0]))
    table_dict_today = info_today[1]
    db_list_yesterday = list(set(info_yesterday[0]))
    table_dict_yesterday = info_yesterday[1]
    for today in db_list_today:
        if today not in db_list_yesterday:
            add_db_list.append(today)
    adding_info['database'] = [len(db_list_today), len(db_list_yesterday), len(add_db_list)]
    adding_info['db_add_detail'] = add_db_list
    table_list_today = []
    table_list_yesterday = []
    for table in table_dict_today:
        for key in table:
            table_list_today.append(key)
    for table in table_dict_yesterday:
        for key in table:
            table_list_yesterday.append(key)
    for table in table_list_today:
        if table not in table_list_yesterday:
            add_table_list.append(table)
    adding_info['table'] = [len(table_list_today), len(table_list_yesterday), len(add_table_list)]
    return adding_info


# 生成hdfs的用户以及目录的增量数据字典
def add_dictinfo(tupe1, tupe2, attrs):
    dict_today = tupe1[1]
    dict_yesterday = tupe2[1]
    user_today = tupe1[0][attrs[0]]
    user_yesterday = tupe2[0][attrs[0]]
    add_dict = dict((attr, []) for attr in attrs)
    sort_add = {}
    for i in range(len(user_today)):
        if user_today[i] in user_yesterday:
            add_dict[attrs[0]].append(user_today[i])
            adding = dict_today.get(user_today[i]) - dict_yesterday.get(user_today[i])
            add_dict[attrs[1]].append(adding)
            sort_add[user_today[i]] = [adding]
        else:
            add_dict[attrs[0]].append(user_today[i])
            adding = dict_today.get(user_today[i])
            add_dict[attrs[1]].append(dict_today.get(user_today[i]))
            sort_add[user_today[i]] = [adding]
    add_info = sorted(sort_add.iteritems(), key=lambda asd: asd[1], reverse=True)
    if attrs[0] == 'joinedpath':
        path_temp = []
        for info in add_info:
            path = info[0]
            if len(path.split('/')[1:]) <= 2 or ".Trash" in path:
                path_temp.append(info)
        for i in range(len(path_temp)):
            add_info.remove(path_temp[i])
    add_top10 = add_info[0:10]
    # 去除前十个中增量为0的数据
    if attrs[0] == 'user':
        add_topN = []
        for top in add_top10:
            if top[1][0] == 0:
                add_topN.append(top)
        if add_topN:
            for i in range(len(add_topN)):
                add_top10.remove(add_topN[i])
    for top in add_top10:
        top[1].insert(0, dict_today[top[0]])
        if top[0] in dict_yesterday.keys():
            top[1].insert(1, dict_yesterday[top[0]])
        else:
            top[1].insert(1, 0)
    return add_top10


# 能量池的变化量
def getUsedrate(query, from_time, to_time, sqParam1, sqParam2):
    attrs = ['entityName', sqParam1, sqParam2, 'used_rate/%']
    html = []
    html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td style='width:300px;'>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")
    responseList = do_query(query, from_time, to_time)
    entityNames = []
    h = []
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                metadata = ts.metadata
                entityName = metadata.attributes['entityName']
                if str(entityName).endswith("group"):
                    entityNames.append(entityName)
    for entityName in entityNames:
        row = []
        tempqy = "SELECT " + sqParam1 + "," + sqParam2 + " WHERE entityName =\"" + entityName + "\"AND category = YARN_POOL"
        row.append("<tr>")
        row.append("<td>")
        row.append(entityName)
        row.append("</td>")
        responseList = do_query(tempqy, from_time, to_time)
        temp = -1
        flag = 0
        for response in responseList:
            if response.timeSeries:
                for ts in response.timeSeries:
                    Value = ts.data[len(ts.data) - 1]
                    row.append("<td>")
                    resultValue = Value.value
                    if sqParam1 == "fair_share_mb":
                        resultValue = '%.1f' % ((float)(resultValue) / 1024)
                        row.append(str(resultValue) + "G")
                    else:
                        row.append('%.1f' % ((float)(10.222222)))
                    row.append("</td>")
                    used_rate = 0
                    if temp != -1:
                        if Value.value != 0:
                            used_rate = '%.2f' % ((float)(temp) / (float)(Value.value))
                            used_rate = int((float)(used_rate) * 100)
                        row.append("<td>")
                        row.append(used_rate)
                        row.append("</td>")
                    if temp == -1:
                        temp = Value.value
                    elif Value.value * 0.8 <= temp:
                        flag = 1
        if flag == 0:
            row = []
        else:
            row.append("</tr>")
        temp = -1
        flag = 0
        if len(row) != 0:
            h.append((used_rate, row))
    result = sorted(h, reverse=True)
    for item in result:
        html += item.__getitem__(1)
    html.append("</table>")
    report = ''
    for vv in html:
        report += str(vv)
    return report


# 拼装邮件的内容，以表格形式显示变化较大的用户/目录对应的增量
def report_user(add_top10, attrs):
    html = []
    html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td style='width:300px;'>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")
    for top in add_top10:
        user = top[0]
        html.append("<tr>")
        html.append("<td>")
        html.append(user)
        html.append("</td>")
        for size in top[1]:
            html.append("<td>")
            if size > 0:
                if ((float)(size) > 1024 * 1024 * 1024 * 1024):
                    size = ('%.2f' % ((float)(size) / 1024 / 1024 / 1024 / 1024)) + "T"
                elif ((float)(size) > 1024 * 1024 * 1024):
                    size = ('%.2f' % ((float)(size) / 1024 / 1024 / 1024)) + "G"
                elif ((float)(size) > 1024 * 1024):
                    size = ('%.2f' % ((float)(size) / 1024 / 1024)) + "M"
                elif ((float)(size) > 1024):
                    size = ('%.2f' % ((float)(size) / 1024)) + "K"
                elif ((float)(size) < 1024):
                    size = ('%.2f' % ((float)(size))) + "B"
                html.append(size)
                html.append("</td>")
        html.append("</tr>")
    html.append("</table>")
    report = ''.join(html)
    return report


# 拼装邮件的内容，以表格形式显示变化较大的目录对应的增量
def report_hdfs(add_top10, attrs):
    html = []
    html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td style='width:300px;'>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")
    for top in add_top10:
        key = top[0]
        values = top[1]
        html.append("<tr>")
        html.append("<td>")
        html.append(key)
        html.append("</td>")
        for value in values:
            html.append("<td>")
            if ((float)(value) > 1024):
                value = ('%.2f' % ((float)(value) / 1024)) + "T"
            else:
                value = str(value) + "G"
            html.append(value)
            html.append("</td>")
        html.append("</tr>")
    html.append("</table>")
    report = ''.join(html)
    return report


# 拼装邮件内容，以表格形式显示数据库以及表的增量信息
def generate_report_adding_info(adding_info):
    html = []
    add_database_list = adding_info['database']
    db_add_detail_list = adding_info['db_add_detail']
    add_tabl_list = adding_info['table']
    attrs = ["类型", "今日数量", "昨日数量", "增量"]
    html.append("<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td style='width:300px;'>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")
    html.append("<tr><td style='width:300px;'>")
    html.append("database")
    html.append("</td>")
    for i in range(len(add_database_list)):
        html.append("<td style='width:300px;'>")
        if i == 2:
            html.append(str(add_database_list[i]))
            if add_database_list[i] != 0:
                html.append("&nbsp;&nbsp;(")
                html.append("<a href='http://bigdata-manager.wanda.f2e-test.cn/datahub/dashboard/resourceConsumption.html'>增长详情</a>")
                html.append(")")
        else:
            html.append(str(add_database_list[i]))
        html.append("</td>")
    html.append("</tr><tr><td style='width:300px;'>")
    html.append("table")
    html.append("</td>")
    for i in range(len(add_tabl_list)):
        html.append("<td style='width:300px;'>")
        if i == 2:
            html.append(str(add_tabl_list[i]) + "&nbsp;&nbsp;(")
            html.append("<a href='http://bigdata-manager.wanda.f2e-test.cn/datahub/dashboard/resourceConsumption.html'>增长详情</a>")
            html.append(")")
        else:
            html.append(str(add_tabl_list[i]))
        html.append("</td>")
    html.append("</tr>")
    html.append("</table>")
    report = ''.join(html)
    return report


# 生成HDFS环比报告-周环比（表格和对应趋势图,按周、月、季度统计）
def getHDFSWeekHistory(query, from_time, to_time, caption, granularity, desired_rollup, must_use_desired_rollup,
                       dfstotal, dfsRemaining):
    html = []
    x_labels = []
    dict = {}
    maxY = 0  # Y轴最大值
    type = 'SAMPLE'  # 数据类型，用于判断是否是计算变量还是原始变量
    unit = None
    rollupUsed = None

    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
        caption = caption + ' (' + from_time.strftime('%m%d') + ')'  # 日报标题
    else:
        caption = caption + ' (' + from_time.strftime('%m%d') + '--' + to_time.strftime('%m%d') + ')'  # 周报标题
    # fileName = caption + SUFIX

    responseList = do_query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                x = []
                y = []
                metadata = ts.metadata
                rollupUsed = metadata.rollupUsed
                unit = metadata.unitNumerators[0].encode("utf-8")
                y_title = metadata.metricName + "(" + metadata.unitNumerators[0].encode("utf-8") + ")"
                for data in ts.data:
                    type = data.type
                    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
                        label = zone_conversion(data.timestamp)
                    elif ((Granularity.HOURLY == granularity) or (Granularity.SIX_HOURS == granularity)):
                        label = zone_conversion(data.timestamp)
                    else:
                        label = zone_conversion(data.timestamp)
                    x_labels.append(label)
                    y.append(data.value)
                    key = metadata.entityName
                    value = data.value
                    dict.setdefault(key, []).append(value)
                maxY = max(maxY, max(y))

    maxY = max(maxY, 1024 * 1024 * 1024 * 1024 * 1024)

    # Y轴单位，例如50%，100%
    if ('SAMPLE' == type):
        # 如果是原始变量，直接根据单位设置Y轴单位后缀
        if ('percent' == unit):
            maxYTick = maxY
            suffix = '%'  # 单位后缀
        elif ('bytes' == unit):
            # 网络流量单位转换
            if (maxY > 1024 * 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024 / 1024
                suffix = 'T'
            elif (maxY > 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024
                suffix = 'G'
            elif (maxY > 1024 * 1024):
                maxYTick = maxY / 1024 / 1024
                suffix = 'M'
            elif (maxY > 1024):
                maxYTick = maxY / 1024
                suffix = 'K'
            else:
                maxYTick = maxY
                suffix = 'b'
    else:
        # 如果是计算变量，默认设置
        maxYTick = maxY
        suffix = '%'

    valListWeek = []
    timeListWeek = []
    increList = []
    totalSize = 0
    list = dict.values()
    if (len(list) > 0 and len(list[0]) > 0):
        if (rollupUsed == u'DAILY'):
            for i in range(len(x_labels)):
                if datetime.datetime.strptime(x_labels[i], '%Y-%m-%d %H:%M').weekday() == 0:
                    break
            valListWeek = list[0][i::7][::-1]  # 按时间由近及远排列，每个元素为周1
            timeListWeek = x_labels[i::7][::-1]
            # valListWeek.append(list[0][::-1][0]) #增加当天时间
            # timeListWeek.append(x_labels[::-1][0])
            # valListWeek = list[0][::-6]  # 按时间由近及远排列
            # timeListWeek = x_labels[::-6]
        elif (rollupUsed == u'WEEKLY'):
            valListWeek = list[0][::-1]  # 按时间由近及远排列
            timeListWeek = x_labels[::-1]
        if (rollupUsed == 'DAILY' or rollupUsed == 'WEEKLY'):
            html.append(
                "<br/><br/>周增长情况<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr><th>日期</th><th>已用容量(T)</th><th>上周容量(T)</th><th>增量(T)</th><th>周增长率(%)</th><th>趋势图</th></tr>")
        for i in range(len(valListWeek) - 1):
            currVal = valListWeek[i]
            lastWeekVal = valListWeek[i + 1]
            html.append("<tr><td>")
            html.append(timeListWeek[i])
            html.append("</td><td>")
            html.append('%.2f' % (currVal / 1024 / 1024 / 1024 / 1024))
            html.append("</td><td>")
            html.append('%.2f' % (lastWeekVal / 1024 / 1024 / 1024 / 1024))
            html.append("</td><td>")
            html.append('%.2f' % ((currVal - lastWeekVal) / 1024 / 1024 / 1024 / 1024))
            increList.append((currVal - lastWeekVal) / 1024 / 1024 / 1024 / 1024)
            html.append("</td><td>")
            if (int(lastWeekVal) != 0):
                html.append('%.2f' % (100 * (currVal - lastWeekVal) / lastWeekVal))
            html.append("</td>")
            if (0 == i):
                html.append("<td rowspan=" + str(len(valListWeek)) + "><img src=cid:id0></td>")
            html.append("</tr>")
        html.append("</table>")
        maxsize = 0
        for size in increList[0:4]:
            maxsize = max(size, maxsize)
        dfstotal = dfstotal / 1024 / 1024 / 1024 / 1024
        dfsRemaining = dfsRemaining / 1024 / 1024 / 1024 / 1024
        if maxsize > 0:
            daysRemaining1 = "%.1f" % ((dfstotal * 0.7 - (dfstotal - dfsRemaining)) / maxsize * 7)
        else:
            daysRemaining1 = 10000
        X_parameters = []
        Y_parameters = []
        for val in list[0]:
            X_parameters.append([val / 1024 / 1024 / 1024 / 1024])
        for i in range(len(X_parameters)):
            Y_parameters.append(i)
        predictions = get_linear_model(X_parameters, Y_parameters, dfstotal * 0.7)
        daysRemaining2 = predictions[0] - len(Y_parameters)
        remark = "说明：按最近增长速度，预计还有<font color=red>" + "%.1f" % min(daysRemaining1,
                                                                  daysRemaining2) + "</font>天到达70%的警戒线<br/>"
        html.append(remark)

        line_chart1 = pygal.Bar(width=800, height=300)
        line_chart1.x_labels = timeListWeek[::-1]
        line_chart1.add('hdfs', valListWeek[::-1])
        line_chart1.y_title = unit
        # 设置Y轴演示标签
        line_chart1.y_labels = [
            {'label': 'O', 'value': 0},
            {'label': "%.1f" % (0.25 * maxYTick) + suffix, 'value': 0.25 * maxY},
            {'label': "%.1f" % (0.5 * maxYTick) + suffix, 'value': 0.5 * maxY},
            {'label': "%.1f" % (0.75 * maxYTick) + suffix, 'value': 0.75 * maxY},
            {'label': "%.1f" % (maxYTick) + suffix, 'value': maxY}]
        line_chart1.render_to_png('hdfsweek.png')
    report = ''.join(html)
    return report


# 生成HDFS环比报告-月环比（表格和对应趋势图,按周、月、季度统计）
def getHDFSMonthHistory(query, from_time, to_time, caption, granularity, desired_rollup, must_use_desired_rollup):
    html = []
    x_labels = []
    dict = {}
    maxY = 0  # Y轴最大值
    type = 'SAMPLE'  # 数据类型，用于判断是否是计算变量还是原始变量
    unit = None
    rollupUsed = None

    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
        caption = caption + ' (' + from_time.strftime('%m%d') + ')'  # 日报标题
    else:
        caption = caption + ' (' + from_time.strftime('%m%d') + '--' + to_time.strftime('%m%d') + ')'  # 周报标题
    # fileName = caption + SUFIX

    responseList = do_query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                x = []
                y = []
                metadata = ts.metadata
                rollupUsed = metadata.rollupUsed
                unit = metadata.unitNumerators[0].encode("utf-8")
                y_title = metadata.metricName + "(" + metadata.unitNumerators[0].encode("utf-8") + ")"
                for data in ts.data:
                    type = data.type
                    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
                        label = zone_conversion(data.timestamp)
                    elif ((Granularity.HOURLY == granularity) or (Granularity.SIX_HOURS == granularity)):
                        label = zone_conversion(data.timestamp)
                    else:
                        label = zone_conversion(data.timestamp)
                    x_labels.append(label)
                    y.append(data.value)
                    key = metadata.entityName
                    value = data.value
                    dict.setdefault(key, []).append(value)
                maxY = max(maxY, max(y))

    maxY = max(maxY, 1024 * 1024 * 1024 * 1024 * 1024)

    # Y轴单位，例如50%，100%
    if ('SAMPLE' == type):
        # 如果是原始变量，直接根据单位设置Y轴单位后缀
        if ('percent' == unit):
            maxYTick = maxY
            suffix = '%'  # 单位后缀
        elif ('bytes' == unit):
            # 网络流量单位转换
            if (maxY > 1024 * 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024 / 1024
                suffix = 'T'
            elif (maxY > 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024
                suffix = 'G'
            elif (maxY > 1024 * 1024):
                maxYTick = maxY / 1024 / 1024
                suffix = 'M'
            elif (maxY > 1024):
                maxYTick = maxY / 1024
                suffix = 'K'
            else:
                maxYTick = maxY
                suffix = 'b'
    else:
        # 如果是计算变量，默认设置
        maxYTick = maxY
        suffix = '%'

    list = dict.values()
    if (len(list) > 0 and len(list[0]) > 0):
        timeListMonth = []
        valListMonth = []
        if (rollupUsed == 'DAILY'):
            valListMonth = list[0][::-30]  # 按时间由近及远排列
            timeListMonth = x_labels[::-30]
        elif (rollupUsed == 'WEEKLY'):
            valListMonth = list[0][::-4]  # 按时间由近及远排列
            timeListMonth = x_labels[::-4]
        if (rollupUsed == 'DAILY' or rollupUsed == 'WEEKLY'):
            html.append(
                "<br>月增长情况<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr><th>日期</th><th>已用容量(T)</th><th>上月容量(T)</th><th>增量(T)</th><th>月增长率(%)</th><th>趋势图</th></tr>")
            for i in range(len(valListMonth) - 1):
                currVal = valListMonth[i]
                lastMonthVal = valListMonth[i + 1]
                html.append("<tr><td>")
                html.append(timeListMonth[i])
                html.append("</td><td>")
                html.append('%.2f' % (currVal / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                html.append('%.2f' % (lastMonthVal / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                html.append('%.2f' % ((currVal - lastMonthVal) / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                if (int(lastMonthVal) != 0):
                    html.append('%.2f' % (100 * (currVal - lastMonthVal) / lastMonthVal))
                html.append("</td>")
                if (0 == i):
                    html.append("<td rowspan=" + str(len(valListMonth)) + "><img src=cid:id1></td>")
                html.append("</tr>")
            html.append("</table>")
        line_chart2 = pygal.Bar(width=800, height=300)
        line_chart2.x_labels = timeListMonth[::-1]
        line_chart2.add('hdfs', valListMonth[::-1])
        line_chart2.y_title = unit
        line_chart2.y_labels = [
            {'label': 'O', 'value': 0},
            {'label': "%.1f" % (0.25 * maxYTick) + suffix, 'value': 0.25 * maxY},
            {'label': "%.1f" % (0.5 * maxYTick) + suffix, 'value': 0.5 * maxY},
            {'label': "%.1f" % (0.75 * maxYTick) + suffix, 'value': 0.75 * maxY},
            {'label': "%.1f" % (maxYTick) + suffix, 'value': maxY}]
        line_chart2.render_to_png('hdfsmonth.png')

    report = ''.join(html)
    return report


# 生成HDFS环比报告-季度环比（表格和对应趋势图,按周、月、季度统计）
def getHDFSQtrHistory(query, from_time, to_time, caption, granularity, desired_rollup, must_use_desired_rollup):
    html = []
    x_labels = []
    dict = {}
    maxY = 0  # Y轴最大值
    type = 'SAMPLE'  # 数据类型，用于判断是否是计算变量还是原始变量
    unit = None
    rollupUsed = None

    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
        caption = caption + ' (' + from_time.strftime('%m%d') + ')'  # 日报标题
    else:
        caption = caption + ' (' + from_time.strftime('%m%d') + '--' + to_time.strftime('%m%d') + ')'  # 周报标题
    # fileName = caption + SUFIX

    responseList = do_query_rollup(query, from_time, to_time, desired_rollup, must_use_desired_rollup)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                x = []
                y = []
                metadata = ts.metadata
                rollupUsed = metadata.rollupUsed
                unit = metadata.unitNumerators[0].encode("utf-8")
                y_title = metadata.metricName + "(" + metadata.unitNumerators[0].encode("utf-8") + ")"
                for data in ts.data:
                    type = data.type
                    if ((Granularity.RAW == granularity) or (Granularity.TEN_MINUTES == granularity)):
                        label = zone_conversion(data.timestamp)
                    elif ((Granularity.HOURLY == granularity) or (Granularity.SIX_HOURS == granularity)):
                        label = zone_conversion(data.timestamp)
                    else:
                        label = zone_conversion(data.timestamp)
                    x_labels.append(label)
                    y.append(data.value)
                    key = metadata.entityName
                    value = data.value
                    dict.setdefault(key, []).append(value)
                maxY = max(maxY, max(y))

    maxY = max(maxY, 1024 * 1024 * 1024 * 1024 * 1024)

    # Y轴单位，例如50%，100%
    if ('SAMPLE' == type):
        # 如果是原始变量，直接根据单位设置Y轴单位后缀
        if ('percent' == unit):
            maxYTick = maxY
            suffix = '%'  # 单位后缀
        elif ('bytes' == unit):
            # 网络流量单位转换
            if (maxY > 1024 * 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024 / 1024
                suffix = 'T'
            elif (maxY > 1024 * 1024 * 1024):
                maxYTick = maxY / 1024 / 1024 / 1024
                suffix = 'G'
            elif (maxY > 1024 * 1024):
                maxYTick = maxY / 1024 / 1024
                suffix = 'M'
            elif (maxY > 1024):
                maxYTick = maxY / 1024
                suffix = 'K'
            else:
                maxYTick = maxY
                suffix = 'b'
    else:
        # 如果是计算变量，默认设置
        maxYTick = maxY
        suffix = '%'

    list = dict.values()
    if (len(list) > 0 and len(list[0]) > 0):
        timeListQtr = []
        valListQtr = []
        if (rollupUsed == 'DAILY'):
            valListQtr = list[0][::-90]  # 按时间由近及远排列
            timeListQtr = x_labels[::-90]
        elif (rollupUsed == 'WEEKLY'):
            valListQtr = list[0][::-12]  # 按时间由近及远排列
            timeListQtr = x_labels[::-12]
        if (rollupUsed == 'DAILY' or rollupUsed == 'WEEKLY'):
            html.append(
                "<br>季度增长情况<br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr><th>日期</th><th>已用容量(T)</th><th>上季度容量(T)</th><th>增量(T)</th><th>季度增长率(%)</th><th>趋势图</th></tr>")
            for i in range(len(valListQtr) - 1):
                currVal = valListQtr[i]
                lastQrtVal = valListQtr[i + 1]
                html.append("<tr><td>")
                html.append(timeListQtr[i])
                html.append("</td><td>")
                html.append('%.2f' % (currVal / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                html.append('%.2f' % (lastQrtVal / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                html.append('%.2f' % ((currVal - lastQrtVal) / 1024 / 1024 / 1024 / 1024))
                html.append("</td><td>")
                if (int(lastQrtVal) != 0):
                    html.append('%.2f' % (100 * (currVal - lastQrtVal) / lastQrtVal))
                html.append("</td>")
                if (0 == i):
                    html.append("<td rowspan=" + str(len(valListQtr)) + "><img src=cid:id2></td>")
                html.append("</tr>")
            html.append("</table>")

        line_chart3 = pygal.Bar(width=800, height=300)
        line_chart3.x_labels = timeListQtr[::-1]
        line_chart3.add('hdfs', valListQtr[::-1])
        line_chart3.y_title = unit
        line_chart3.y_labels = [
            {'label': 'O', 'value': 0},
            {'label': "%.1f" % (0.25 * maxYTick) + suffix, 'value': 0.25 * maxY},
            {'label': "%.1f" % (0.5 * maxYTick) + suffix, 'value': 0.5 * maxY},
            {'label': "%.1f" % (0.75 * maxYTick) + suffix, 'value': 0.75 * maxY},
            {'label': "%.1f" % (maxYTick) + suffix, 'value': maxY}]
        line_chart3.render_to_png('hdfsquarter.png')

    report = ''.join(html)
    return report


def getJobCount(query, from_time, to_time):
    jobCount = 0
    responseList = do_query(query, from_time, to_time)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                for data in ts.data:
                    jobCount += 1
    return jobCount


def getImpalaJobSummary(from_time, to_time):
    query_1min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration <= 60000.0"
    query_5min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 60000.0 and query_duration <= 300000.0"
    query_15min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 300000.0 and query_duration <= 900000.0"
    query_30min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 900000.0 and query_duration <= 1800000.0"
    query_60min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 1800000.0 and query_duration <= 3600000.0"
    query_120min_count = "select  query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 3600000.0 and query_duration <= 7200000.0"
    query_120min_plus_count = "select query_duration from IMPALA_QUERIES where serviceName=impala AND (query_state=FINISHED OR query_state=EXCEPTION)  and query_duration > 7200000.0"

    job_1min_count = getJobCount(query_1min_count, from_time, to_time)
    job_5min_count = getJobCount(query_5min_count, from_time, to_time)
    job_15min_count = getJobCount(query_15min_count, from_time, to_time)
    job_30min_count = getJobCount(query_30min_count, from_time, to_time)
    job_60min_count = getJobCount(query_60min_count, from_time, to_time)
    job_120min_count = getJobCount(query_120min_count, from_time, to_time)
    job_120min_plus_count = getJobCount(query_120min_plus_count, from_time, to_time)
    job_total = job_1min_count + job_5min_count + job_15min_count + job_30min_count + job_60min_count + job_120min_count + job_120min_plus_count

    types = '0-1m', '1-5m', '5-15m', '15-30m', '30-60m', '60-120m', '>120m'
    X = [job_1min_count, job_5min_count, job_15min_count, job_30min_count, job_60min_count, job_120min_count,
         job_120min_plus_count]
    pie_chart = pygal.Pie(width=800, height=400)
    pie_chart.title = "Total Count:" + str(job_total)
    for i in range(min(len(types), len(X))):
        if (X[i] != 0):
            label = types[i] + ':' + str(X[i]) + ' ' + "%.1f" % ((float)(X[i]) / job_total * 100) + '%'
            pie_chart.add(label, X[i])
    pie_chart_name = "impalaJobSummary.png"
    pie_chart.render_to_png(pie_chart_name)
    return pie_chart_name


def getHiveJobSummary(from_time, to_time):
    query_5min_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration <= 300000.0 "
    query_15min_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration > 300000.0 and application_duration <= 900000.0 "
    query_30min_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration > 900000.0 and application_duration <= 1800000.0 "
    query_60min_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration > 1800000.0 and application_duration <= 3600000.0 "
    query_120min_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration > 7200000.0 and application_duration <= 7200000.0 "
    query_120min_plus_count = "select application_duration from YARN_APPLICATIONS where service_name = \"yarn\" and hive_query_id RLIKE \".*\" and application_duration > 7200000.0 "

    job_5min_count = getJobCount(query_5min_count, from_time, to_time)
    job_15min_count = getJobCount(query_15min_count, from_time, to_time)
    job_30min_count = getJobCount(query_30min_count, from_time, to_time)
    job_60min_count = getJobCount(query_60min_count, from_time, to_time)
    job_120min_count = getJobCount(query_120min_count, from_time, to_time)
    job_120min_plus_count = getJobCount(query_120min_plus_count, from_time, to_time)
    job_total = job_5min_count + job_15min_count + job_30min_count + job_60min_count + job_120min_count + job_120min_plus_count

    types = '1-5m', '5-15m', '15-30m', '30-60m', '60-120m', '>120m'
    X = [job_5min_count, job_15min_count, job_30min_count, job_60min_count, job_120min_count, job_120min_plus_count]
    pie_chart = pygal.Pie(width=800, height=400)
    pie_chart.title = "Total Count:" + str(job_total)
    for i in range(min(len(types), len(X))):
        if (X[i] != 0):
            label = types[i] + ':' + str(X[i]) + ' ' + "%.1f" % ((float)(X[i]) / job_total * 100) + '%'
            pie_chart.add(label, X[i])
    pie_chart_name = "hiveJobSummary.png"
    pie_chart.render_to_png(pie_chart_name)
    return pie_chart_name


# 生成Impala Top20报告
def getImpalaTop20(query, from_time, to_time, caption, granularity):
    h = []
    attrs = ['user', 'database', 'query_duration', 'thread_cpu_time', 'hdfs_bytes_read', 'memory_accrual',
             'memory_aggregate_peak', 'category', 'executing', 'service_name', 'coordinator_host_id', 'stats_missing',
             'statement', 'entityName', 'pool']

    html = []
    html.append("<br><br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    html.append("<td>time</td>")
    for attr in attrs:
        html.append("<td>")
        html.append(attr)
        if ('query_duration' == attr):
            html.append("(ms)")
        html.append("</td>")
    html.append("</tr>")

    responseList = do_query(query, from_time, to_time)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                metadata = ts.metadata
                for data in ts.data:
                    line = []
                    if metadata.attributes:
                        line.append("<tr>")
                        line.append("<td>")
                        line.append(utc2local(data.timestamp).strftime("%m-%d %H:%M:%S"))
                        line.append("</td>")
                        for attr in attrs:
                            line.append("<td>")
                            if metadata.attributes.has_key(attr):
                                attrVal = metadata.attributes[attr]
                                # 自动转换时长单位
                                if ('query_duration' == attr):
                                    if ((int)(attrVal) > 60 * 60 * 1000):
                                        attrVal = ('%.2f' % ((float)(attrVal) / 60 / 60 / 1000)) + "h"  # 小时
                                    elif ((int)(attrVal) > 60 * 1000):
                                        attrVal = (str)((int)(attrVal) / 60 / 1000) + "m"  # 分
                                    elif ((int)(attrVal) > 1000):
                                        attrVal = (str)((int)(attrVal) / 1000) + "s"  # 秒
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
                                line.append(attrVal)
                            else:
                                line.append('N/A')
                            line.append("</td>")
                        line.append("</tr>")
                        heapq.heappush(h, (
                        data.value, line))  # 根据时长进行堆排序，Push的格式为(duration,<tr><td>attr1<td>...td>attrN<td></tr>)
    top20 = sorted(h, reverse=True)[0:20]
    for item in top20:
        html += item.__getitem__(1)
    html.append("</table>")
    report = ''.join(html)
    return report


# 生成Hive Top20报告
# def getHive20(query,from_time,to_time,caption,granularity):
#   h = []
#   attrs = ['user','name','application_duration','cpu_milliseconds','mb_millis','hdfs_bytes_read','category','service_name','entityName','pool']
#
#   html = []
#   html.append("<br><br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
#   html.append("<td>time</td>")
#   for attr in attrs:
#     html.append("<td>")
#     html.append(attr)
#     html.append("</td>")
#   html.append("</tr>")
#
#   responseList = do_query(query, from_time, to_time)
#   for response in responseList:
#     if response.timeSeries:
#       for ts in response.timeSeries:
#         metadata = ts.metadata
#         for data in ts.data:
#           line = []
#           if metadata.attributes:
#             line.append("<tr>")
#             line.append("<td>")
#             line.append(utc2local(data.timestamp).strftime("%m-%d %H:%M:%S"))
#             line.append("</td>")
#             for attr in attrs:
#               line.append("<td>")
#               if metadata.attributes.has_key(attr):
#                 attrVal = metadata.attributes[attr]
#                 # 自动转换时长单位
#                 if ('application_duration' == attr):
#                   if(int((float)(attrVal)) > 60 * 60 * 1000):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 60 / 60 / 1000)) + "h" # 小时
#                   elif (int((float)(attrVal)) > 60 * 1000):
#                     attrVal = (str)(int((float)(attrVal)) / 60 / 1000) + "m" # 分
#                   elif (int((float)(attrVal)) > 1000):
#                     attrVal = (str)(int((float)(attrVal)) / 1000)  + "s"# 秒
#                 if ('mb_millis' == attr):
#                   if ((float)(attrVal) > 8 * 1024 * 1024 * 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024 / 1024)) + "G"
#                   elif ((float)(attrVal) > 8 * 1024 * 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024 / 1024)) + "M"
#                   elif ((float)(attrVal) > 8 * 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 8 / 1024)) + "K"
#                 if ('hdfs_bytes_read' == attr):
#                   if ((float)(attrVal) > 1024 * 1024 * 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 1024 / 1024 / 1024)) + "G"
#                   elif ((float)(attrVal) > 1024 * 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 1024 / 1024)) + "M"
#                   elif ((float)(attrVal) > 1024):
#                     attrVal = ('%.2f' % ((float)(attrVal) / 1024)) + "K"
#                 line.append(attrVal)
#               else:
#                 line.append(' ')
#               line.append("</td>")
#             line.append("</tr>")
#             heapq.heappush(h,(data.value,line))  #根据时长进行堆排序，Push的格式为(duration,<tr><td>attr1<td>...td>attrN<td></tr>)
#   top20 = sorted(h, reverse=True)[0:20]
#   for item in top20:
#     html += item.__getitem__(1)
#   html.append("</table>")
#   report = ''.join(html)
#   return report

class HiveInfo(ApiClient):
    """

    """

    def get_top_user_demo(self, from_time, to_time, duration=900):
        filter_str = "hive_query_id RLIKE \".*\" and application_duration >= %ss" % duration
        return get_service(self._api, cluster_name="cluster", name="yarn").get_yarn_applications(start_time=from_time,
                                                                                                 end_time=to_time,
                                                                                                 filter_str=filter_str)


def do_get_top_user_demo(from_time, to_time, duration=900):
    attrs = ['user', 'application_duration', 'time', 'category', 'service_name', 'name', 'entityName']

    html = []
    html.append("<br><br><table bgcolor=#F9F9F9 border=1 cellspacing=0><tr>")
    for attr in attrs:
        html.append("<td>")
        html.append(attr)
        html.append("</td>")
    html.append("</tr>")

    hive_info = HiveInfo()
    top_users = hive_info.get_top_user_demo(from_time=from_time, to_time=to_time, duration=duration)
    massage_dfs = []
    if top_users.applications:
        for i in top_users.applications:
            line = {}
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
            massage_dfs.append(line)
    top20 = sorted(massage_dfs, key=lambda t: t['application_duration1'], reverse=True)

    for item in top20:
        html.append("<tr>")
        for attr in attrs:
            html.append("<td>")
            html.append(item.__getitem__(attr))
            html.append("</td>")
        html.append("</tr>")

    html.append("</table>")
    report = ''.join(html)
    return report


# 获取文件系统总容量
def getDfsCapacity(query, from_time, to_time):
    dfscapacity = 0
    unit = None

    responseList = do_query(query, from_time, to_time)
    for response in responseList:
        if response.timeSeries:
            for ts in response.timeSeries:
                for data in ts.data:
                    dfscapacity = data.value
    return dfscapacity


def querySmallFiles(ip, user, password, command):
    html = []
    html.append("<br><br><table bgcolor=#F9F9F9 border=1 cellspacing=0>")

    client = ssh.SSHClient()
    client.set_missing_host_key_policy(ssh.AutoAddPolicy())
    client.connect(ip, port=22, username=user, password=password)
    stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read()
    rows = out.split("\n")
    for row in rows:
        if (row.endswith('+') == False):
            cols = row.split("|")
            html.append("<tr>")
            for col in cols:
                if ((col != ',') and (col != '')):
                    html.append("<td>")
                    html.append(col)
                    html.append("</td>")
            html.append("</tr>")
    html.append("</table>")
    report = ''.join(html)
    return report


def queryFileIncreInfo(ip, user, password, command):
    timeList = []
    valfilesList = []
    valsizeList = []
    client = ssh.SSHClient()
    client.set_missing_host_key_policy(ssh.AutoAddPolicy())
    client.connect(ip, port=22, username=user, password=password)
    stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read()
    rows = out.split("\n")
    for row in rows[3::]:
        if (row.endswith('+') == False):
            cols = row.split("|")
            if (len(cols) > 3):
                timeList.append(cols[1])
                valsizeList.append(float(cols[2]))
                valfilesList.append(float(cols[3]))

    line_chart6 = pygal.HorizontalBar(width=850, height=800)
    line_chart6.x_labels = timeList
    line_chart6.add('File Count', valsizeList)
    line_chart6.render_to_png('num_of_files.png')

    line_chart7 = pygal.HorizontalBar(width=650, height=800)
    line_chart7.x_labels = timeList
    line_chart7.add('Total size(G)', valfilesList)
    line_chart7.render_to_png('total_size_gb.png')


def main(argv):
    now = datetime.datetime.now()
    one_Day_Ago = now - datetime.timedelta(days=1)  # 前一天
    two_Quarter_Ago = now - datetime.timedelta(days=180)  # 前两个季度
    today = datetime.date.today()
    today_string = today.strftime('%Y-%m-%d')
    one_Day_Later_string = (today + datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 明天
    one_Day_Ago_string = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 前一天
    lastFriday = utils.getDate.get_lastFriday(now)  # 最近周五的00:00:00

    # 获取HDFS增量数据
    attrs_user = ["用户", "今日容量", "昨日容量", "增量"]
    attrs_contents = ["目录", "今日容量", "昨日容量", "增量"]
    attr_user = ['user', 'add']
    attr_contents = ['joinedpath', 'add']
    command_today = "curl -k -X GET -u 'xxx:xxx' -i 'http://xxx:7180/api/v13/clusters/cluster/services/hdfs/reports/hdfsUsageReport?" \
                    "aggregation=DAILY&from=" + today_string + "&to=" + one_Day_Later_string + "'"
    command_yesterday = "curl -k -X GET -u 'xxx:xxx' -i 'http://xxx:7180/api/v13/clusters/cluster/services/hdfs/reports/hdfsUsageReport?" \
                        "aggregation=DAILY&from=" + one_Day_Ago_string + "&to=" + today_string + "'"
    sql_today = "select * from idc_infrastructure_db.hdfs_meta_dir_all_daily where dt='" + today_string + "'and size>5"
    sql_yesterday = "select * from idc_infrastructure_db.hdfs_meta_dir_all_daily where dt='" + one_Day_Ago_string + "' and size>5"
    resource_Vcore = getUsedrate("SELECT max_share_vcores WHERE category = YARN_POOL", one_Day_Ago, now,
                                 "fair_share_vcores",
                                 "max_share_vcores")
    resource_Memory = getUsedrate("SELECT max_share_mb WHERE category = YARN_POOL", one_Day_Ago, now, "fair_share_mb",
                                  "max_share_mb")
    hive_table_sql_today = "select * from idc_infrastructure_db.hive_table_info_all_daily where dt='" + today_string + "'"
    hive_table_sql_yesterday = "select * from idc_infrastructure_db.hive_table_info_all_daily where dt='" + one_Day_Ago_string + "'"
    report_today = queryAddFiles(command_today)
    report_yesterday = queryAddFiles(command_yesterday)
    add_info = add_dictinfo(report_today, report_yesterday, attr_user)
    hdfsAdding_user = report_user(add_info, attrs_user)
    contents_today = quer_contents(sql_today)
    contents_yesterday = quer_contents(sql_yesterday)
    add_content = add_dictinfo(contents_today, contents_yesterday, attr_contents)
    hdfsAdding_contents = report_hdfs(add_content, attrs_contents)
    hive_tbl_today = query_hive_table(hive_table_sql_today)
    hive_tbl_yesterday = query_hive_table(hive_table_sql_yesterday)
    hive_adding_info = do_get_info_adding(hive_tbl_today, hive_tbl_yesterday)
    report_table_adding = generate_report_adding_info(hive_adding_info)

    # 获取DFS信息
    dfs_capacity = getDfsCapacity("select dfs_capacity where  entityName=hdfs:nn-idc", None, None)
    dfs_capacity_used = getDfsCapacity("select dfs_capacity_used where  entityName=hdfs:nn-idc", None, None)
    dfs_capacity_used_non_hdfs = getDfsCapacity("select dfs_capacity_used_non_hdfs  where  entityName=hdfs:nn-idc",
                                                None, None)
    dfsRemaining = dfs_capacity - dfs_capacity_used - dfs_capacity_used_non_hdfs

    # 群集CPU利用率-日报
    fileCPU = getReportChart("SELECT cpu_percent_across_hosts WHERE category = CLUSTER",
                             one_Day_Ago, now, CAPTIONCPU, filenameCPU, Granularity.RAW, 'HOURLY', True)
    # 群集内存使用率-日报
    fileMEM = getReportChart(
        "SELECT 100 * total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts WHERE category=CLUSTER",
        one_Day_Ago, now, CAPTIONMEM, filenameMEM, Granularity.RAW, 'HOURLY', True)
    # 群集网络传输量-日报
    fileNET = getReportChart("select total_bytes_transmit_rate_across_network_interfaces where category = CLUSTER",
                             one_Day_Ago, now, CAPTIONNET, filenameNET, Granularity.RAW, 'HOURLY', True)
    # 群集DFS使用量-周环比
    fileHDFSWeek = getHDFSWeekHistory("select dfs_capacity_used where  entityName=hdfs:nn-idc", two_Quarter_Ago, now,
                                      'Hadoop Cluster HDFS Report', Granularity.DAILY, 'DAILY', True, dfs_capacity,
                                      dfsRemaining)
    # 群集DFS使用量-月环比
    fileHDFSMonth = getHDFSMonthHistory("select dfs_capacity_used where  entityName=hdfs:nn-idc", two_Quarter_Ago,
                                        lastFriday,
                                        'Hadoop Cluster HDFS Report', Granularity.DAILY, 'WEEKLY', True)
    # 群集DFS使用量-季度环比
    fileHDFSQuarter = getHDFSQtrHistory("select dfs_capacity_used where  entityName=hdfs:nn-idc", two_Quarter_Ago,
                                        lastFriday,
                                        'Hadoop Cluster HDFS Report', Granularity.DAILY, 'WEEKLY', True)

    picImpalaJobSummary = getImpalaJobSummary(one_Day_Ago, now)
    # IMPALA-运行时间超过5分钟的任务-日报
    impalaTop20 = getImpalaTop20(
        "select query_duration from IMPALA_QUERIES where service_name = impala and query_duration >= 300000.0",
        one_Day_Ago, now, "Impala Top 20", Granularity.RAW)

    picHiveJobSummary = getHiveJobSummary(one_Day_Ago, now)

    hiveTop20 = do_get_top_user_demo(one_Day_Ago, now)

    pwd = os.getcwd()
    picList = [pwd + "//" + 'hdfsweek.png', pwd + "//" + 'hdfsmonth.png', pwd + "//" + 'hdfsquarter.png',
               pwd + "//" + fileCPU, pwd + "//" + fileMEM, \
               pwd + "//" + fileNET, pwd + "//" + 'num_of_files.png', pwd + "//" + 'total_size_gb.png',
               pwd + "//" + picImpalaJobSummary, pwd + "//" + picHiveJobSummary]

    mail_msg = "<h1>综合生产集群报告</h1>"
    mail_msg += "文件系统概况：<br/>" + "总容量：" + "%.1f" % (dfs_capacity / 1024 / 1024 / 1024 / 1024) + "T" \
                + "(当前已用HDFS容量：" + "%.1f" % (dfs_capacity_used / 1024 / 1024 / 1024 / 1024) + "T" \
                + "，已用非HDFS容量：" + "%.1f" % (dfs_capacity_used_non_hdfs / 1024 / 1024 / 1024 / 1024) + "T" \
                + "，剩余容量：" + "%.1f" % (dfsRemaining / 1024 / 1024 / 1024 / 1024) + "T" \
                + "，使用率：" + "%.1f" % ((1 - dfsRemaining / dfs_capacity) * 100) + "%)"
    mail_msg += "<br/>NameNode:2个,Datanode：50个<br/>"

    mail_msg += fileHDFSWeek + fileHDFSMonth + fileHDFSQuarter

    cmd = "impala-shell -i xxx:21000 -l --auth_creds_ok_in_clear -u xxx --ldap_password_cmd=\"printf xxx\" \
        -q \"select to_date(t.day),t.num_of_files,t.total_size_gb \
        from ( \
            select trunc(modification_time,'DD') day,count(1) num_of_files, round(sum(filesize)/1024/1024/1024,2) total_size_gb \
            from idc_infrastructure_db.hdfs_meta where trunc(modification_time,'DD') is not null \
            group by trunc(modification_time,'DD') \
            order by trunc(modification_time,'DD') desc limit 30 \
        )t \
        order by t.day ;\" "
    queryFileIncreInfo("xxx", 'xxx', 'xxx', cmd)
    mail_msg += "<br><br>每日文件增长数和大小<div><img src=cid:id6" + "><img src=cid:id7" + "></div>"

    mail_msg += "<br><br>群集CPU使用情况<p><img src=cid:id3" + "></p><br>"
    mail_msg += "<br><br>综合生产集群内存使用情况<p><img src=cid:id4" + "></p>"
    mail_msg += "<br><br>综合生产集群网络使用情况<p><img src=cid:id5" + "></p>"
    mail_msg += "资源池具体使用情况（Vcore使用超过80%）：</br>"
    mail_msg += resource_Vcore
    mail_msg += "<br>资源池具体使用情况（Memory使用超过80%）：</br>"
    mail_msg += resource_Memory
    mail_msg += "<br>HDFS日增量用户情况：</br>"
    mail_msg += hdfsAdding_user
    mail_msg += "<br>HDFS日增量目录情况：</br>"
    mail_msg += hdfsAdding_contents
    mail_msg += "<br>Hive/Impala 数据库以及表数据日增量统计</br>"
    mail_msg += report_table_adding
    smallFiles = querySmallFiles("10.214.128.68", 'yuanbowen1', '523180',
                                 "impala-shell -i xxx:21000 -l --auth_creds_ok_in_clear -u xxx --ldap_password_cmd=\"printf xxx\" -q \"select db_name,tbl_name,tbl_owner,support_person,table_location,storage_format,file_size_type,small_files_count from idc_infrastructure_db.hdfs_small_files_result order by small_files_count desc limit 20\";")
    mail_msg += "<br>Top20小文件数"
    mail_msg += smallFiles

    mail_msg += "<br>IMPALA-任务执行时长统计<p><img src=cid:id8 style=\"vertical-align:middle;\"" + "></p>"
    mail_msg += "<br>IMPALA-运行时间超过5分钟的任务"
    if (impalaTop20.count("</tr>") > 1):
        mail_msg += impalaTop20
    else:
        mail_msg += "<br>没有超时任务，运行正常"

    mail_msg += "<br>Hive-任务执行时长统计<p><img src=cid:id9 style=\"vertical-align:middle;\"" + "></p>"
    mail_msg += "<br>Hive-运行时间超过15分钟的任务"
    if (hiveTop20.count("</tr>") > 1):
        mail_msg += hiveTop20
    else:
        mail_msg += "<br>没有超时任务，运行正常"
    print mail_msg
    TO = ['xxx']
    ACC = []
    sendmail(FROM, TO, ACC, SUBJECT, mail_msg, picList)

    for pic in picList:
        os.remove(pic)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
