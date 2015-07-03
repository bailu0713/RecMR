#coding:UTF-8
'''
Created on 13 Aug 2014

@author: Administrator
'''

import simplejson as sj

import sys
reload(sys)
sys.setdefaultencoding("utf8")

import datetime
insertime = datetime.datetime.now()

import pymongo
pyconn = pymongo.Connection(host='192.168.0.135',port=27017)

import MySQLdb
mysqlconn = MySQLdb.connect(host = '192.168.0.136', user = 'ire', passwd = 'ZAQ!XSW@CDE#', db = 'ire', charset = 'utf8')
mysqlcursor = mysqlconn.cursor()

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ire_newcontent(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentid VARCHAR(255), assetId VARCHAR(255), pid VARCHAR(255), contentname VARCHAR(255),
    channelname VARCHAR(255), contentpic VARCHAR(255), main_rank int(11), sub_rank int(11), score bigint, systype int(11),
    status int(11), main_type int(11), sub_type int(11), OSversion double(4,2), isMainLock int(11), isSubLock int(11), create_date DATETIME) charset=utf8
    ''')

mysqlcursor.execute("UPDATE ire_newcontent set `status`=1 where `status`=0;")
mysqlcursor.execute("delete from ire_newcontent where `status`=1")

dateName = str(datetime.datetime.now())
nameindex = dateName.index('.')
dateName = dateName[:nameindex]
dateName = dateName.replace('-', '')
dateName = dateName.replace(':', '')
dateName = dateName.replace(' ', '')
textName = '/home/cibn/output/newContent/newContent'+dateName+'.txt'

with open(textName,'w') as fp:
    fp.write('[')
    outputDic = dict()
    topnlist = list()
    
    sortedList = eval('''pyconn.cibn.content_list.find().sort("createDate",pymongo.DESCENDING).batch_size(30)''')
    count = 0
    cidList = list()
    for doc in sortedList:
        tempDic = dict()
        
        cid = str(doc['cid'])
        if cid in cidList:
            continue
        cidList.append(cid)
        pycursor = pyconn.cibn.content_list.find({'cid':cid}).batch_size(30)
        for doc in pycursor:
            count+=1
            tempInsert = list()
            tempInsert.append(cid)
            tempInsert.append('')
            tempInsert.append('')
            tempInsert.append(str(doc['videoName']))
            tempInsert.append('')
            tempInsert.append(str(doc['videoPic']))
            tempInsert.append('0')
            tempInsert.append(str(count))
            tempInsert.append('0')
            tempInsert.append('1')
            tempInsert.append('0')
            # vod tv type 2-1
            tempInsert.append('2')
            tempInsert.append('1')
            tempInsert.append('')
            tempInsert.append('0')
            tempInsert.append('0')
            tempInsert.append(str(insertime))
            tempDic['count'] = str(count)
            tempDic['cid'] = cid
            tempDic['name'] = str(doc['videoName'])
            tempDic['pic'] = str(doc['videoPic'])
            topnlist.append(tempDic)
            mysqlcursor.execute("insert into ire_newcontent(contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , tempInsert)
            mysqlconn.commit()
            break
        if count>=20:
            break
        
        pycursor.close()
    outputDic['topn'] = topnlist
    outputstring = sj.dumps(outputDic)    
    fp.write(outputstring)
    fp.write(']')
    sortedList.close()
    
mysqlcursor.close()
mysqlconn.close()

pyconn.close()
