#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf8")

import datetime

import simplejson as sj
import pymongo
pyconn = pymongo.MongoClient(host='172.16.11.166',port=27017)

targetId = dict()

#import logging
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename='mfdb.log', filemode='a')
#infologger = logging.getLogger()

import MySQLdb
mysqlconn = MySQLdb.connect(host = '172.16.11.166',db = 'ire', charset = 'utf8')
mysqlcursor = mysqlconn.cursor()

with open('/opt/IAE/contentSimilar/contentRec.txt','r') as fp2:
    temp2 = fp2.readlines()
    for line in temp2:
        line = line.strip().split('//')
        if len(line[1])<=0:
            continue
        if line[0] not in targetId:
            targetId[line[0]] = dict()
            ids = line[1].split(',')
            scores = line[2].split(',')
            reasons = line[3].split(',')
            for idss, score, reason in zip(ids,scores,reasons):
                try:
                    score = int(score)
                except:
                    score = 0
                targetId[line[0]][idss] = dict()
                targetId[line[0]][idss]['id'] = idss
                targetId[line[0]][idss]['score'] = score
                targetId[line[0]][idss]['reason'] = reason
        else:
            ids = line[1].split(',')
            scores = line[2].split(',')
            reasons = line[3].split(',')
            for idss, score, reason in zip(ids,scores,reasons):
                try:
                    score = int(score)
                except:
                    score = 0
                if idss in targetId[line[0]]:
                    targetId[line[0]][idss]['score']+=score
                else: 
                    targetId[line[0]][idss] = dict()                   
                    targetId[line[0]][idss]['id'] = idss
                    targetId[line[0]][idss]['score'] = score
                targetId[line[0]][idss]['reason'] = reason

insertime = datetime.datetime.now()

# json file
dateName = str(datetime.datetime.now())
dateName=dateName.split(" ")[0]
#nameindex = dateName.index('.')
#dateName = dateName[:nameindex]
#dateName = dateName.replace('-', '')
#dateName = dateName.replace(':', '')
#dateName = dateName.replace(' ', '')
textName = '/ftp/CTVIT/content/recResult'+dateName+'.txt'
fp = open(textName,'w')
fp.write('[')

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ire_content_prop(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentid VARCHAR(255), contentname VARCHAR(255), rec_id VARCHAR(255), rec_assetId VARCHAR(255),rec_name VARCHAR(255), rec_channelname VARCHAR(255), rec_pid VARCHAR(255), rec_pic VARCHAR(255), score bigint, reason VARCHAR(255),systype int(11), status int(11), TV_type VARCHAR(255), OSversion double(4,2), isLock int(11), main_type int(11), sub_type int(11),create_date DATETIME) charset=utf8''')

# update status
mysqlcursor.execute("UPDATE ire_content_prop set `status`=1 where systype=1 and `status`=0;")
mysqlcursor.execute("delete from ire_content_prop where `status`=1 and systype=1;")

commaCount = 0
for target, lists in targetId.iteritems():
    lists = sorted(lists.items(), key = lambda d:d[1], reverse = True)
    tempOrigin = dict()
    if commaCount==0:
        commaCount=1
    else:
        fp.write(',')
    pycursor = pyconn.cibn.content_list.find({'cid':target}).batch_size(30)
    oriName = ''
    for doc in pycursor:
        tempOrigin['videoName'] = doc['videoName']
        tempOrigin['videoBrief'] = doc['videoBrief']
        tempOrigin['actors'] = doc['actors']
        tempOrigin['director'] = doc['director']
        tempOrigin['classCode'] = doc['classCode']
        tempOrigin['videoPic'] = doc['videoPic']
        oriName = doc['videoName']
        break
    tempOrigin['contentId'] = target
    insertList = list()
    tempDic = list()
    for listss in lists:
        listss = listss[0]
        tempRec = dict()
#         pk+=1
        tempInsert = list()
        pycursor = pyconn.cibn.content_list.find({'cid':listss}).batch_size(30)
        targetInfo = dict()
        for doc in pycursor:
            targetInfo = doc
            tempRec['videoName'] = doc['videoName']
            tempRec['videoBrief'] = doc['videoBrief']
            tempRec['actors'] = doc['actors']
            tempRec['director'] = doc['director']
            tempRec['classCode'] = doc['classCode']
            tempRec['videoPic'] = doc['videoPic']
            break
#         tempInsert.append(pk)
        tempRec['contentId'] = listss
        tempRec['score'] = targetId[target][listss]['score']
        tempDic.append(tempRec)
        tempInsert.append(target)
        tempInsert.append(oriName)
        tempInsert.append(listss)
        tempInsert.append('')
        tempInsert.append(targetInfo['videoName'])
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(targetId[target][listss]['score'])
        tempInsert.append(targetId[target][listss]['reason'])
        tempInsert.append('1')
        tempInsert.append('0')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('0')
        tempInsert.append('2')
        tempInsert.append('2')
        tempInsert.append(insertime)
        insertList.append(tuple(tempInsert))
#     print insertList
    mysqlcursor.executemany("insert into ire_content_prop(contentid, contentname, rec_id, rec_assetId, rec_name, rec_channelname, rec_pid, rec_pic, score, reason, systype, status, TV_type, OSversion, isLock, main_type, sub_type, create_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , (insertList))
    mysqlconn.commit()
    tempOrigin['reclist'] = tempDic
    fp.write(sj.dumps(tempOrigin,ensure_ascii=False))
    fp.write('\n')

mysqlcursor.close()
mysqlconn.close()

fp.write(']')
fp.close()
