# -*- coding:utf-8 -*-
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header
from utils.getConfig import get_conf

HOST = get_conf("mail","HOST")
USER = get_conf("mail","USER")
PASSWORD = get_conf("mail","PASSWORD")
PORT = get_conf("mail","PORT")


def sendmail(sender, to, cc, subject, message, picList):
    '''
    :param message: 邮件正文
    :param picList: 邮件中的图片文件名列表
    '''
    def addimg(src, imgid):
        fp = open(src, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()
        msgImage.add_header('Content-ID', imgid)
        return msgImage
    flag = False
    msgRoot = MIMEMultipart('related')
    msgtext = MIMEText(message, "html", "utf-8")
    msgRoot.attach(msgtext)
    for i in range(len(picList)):
        id = "<id" + str(i) + ">"
        msgRoot.attach(addimg(picList[i], id))  # 全文件路径，后者为ID 根据ID在HTML中插入的位置
    msgRoot['Subject'] = Header(subject, 'utf8').encode()
    msgRoot['From'] = sender
    msgRoot['To'] = ",".join(to)
    msgRoot['Cc'] = ",".join(cc)

    try:
        server = smtplib.SMTP()
        server.connect(HOST, PORT)
        server.starttls()
        server.login(USER, PASSWORD)
        server.sendmail(sender, to+cc, msgRoot.as_string())
        server.quit()
        print "True"
    except Exception, e:
        print "False"
    return flag

