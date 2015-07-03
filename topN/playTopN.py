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

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ire_content_topn(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentid VARCHAR(255), assetId VARCHAR(255), pid VARCHAR(255), contentname VARCHAR(255),
    channelname VARCHAR(255), contentpic VARCHAR(255), main_rank int(11), sub_rank int(11), score bigint, systype int(11),
    status int(11), main_type int(11), sub_type int(11), OSversion double(4,2), isMainLock int(11), isSubLock int(11), create_date DATETIME) charset=utf8
    ''')

# calculate the rate
reducer =    '''
            function(obj, prev){
                prev.rate+=1;
                }  
            '''
from bson.son import SON

dateName = str(datetime.datetime.now())
nameindex = dateName.index('.')
dateName = dateName[:nameindex]
dateName = dateName.replace('-', '')
dateName = dateName.replace(':', '')
dateName = dateName.replace(' ', '')
textName = '/home/cibn/output/contentTopN/contentTopN'+dateName+'.txt'

with open(textName,'w') as fp:
    fp.write('[')
    outputDic = dict()
    topnlist = list()
    
    result = eval('''pyconn.cibn.user_behavior_play.group(key={"cid":1}, condition={}, initial={"rate": 0}, reduce=reducer)''')
    topList = dict()
    for item in result:
        topList[item['cid']] = item['rate']
    sortedList = sorted(topList.items(), key = lambda d:d[1], reverse = True)
    
    count = 0
    for (cid, score) in sortedList:
        tempDic = dict()
        cid = str(cid)
        pycursor = pyconn.cibn.content_list.find({'cid':cid}).batch_size(30)
        for doc in pycursor:
            count+=1
            tempDic['count'] = str(count)
            tempDic['cid'] = cid
            tempDic['name'] = str(doc['videoName'])
            tempDic['pic'] = str(doc['videoPic'])
            topnlist.append(tempDic)
            
            tempInsert = list()
            tempInsert.append(cid)
            tempInsert.append('')
            tempInsert.append('')
            tempInsert.append(str(doc['videoName']))
            tempInsert.append('')
            tempInsert.append(str(doc['videoPic']))
            tempInsert.append('0')
            tempInsert.append(str(count))
            tempInsert.append(str(score))
            tempInsert.append('1')
            tempInsert.append('0')
            # vod tv type 2-1
            tempInsert.append('2')
            tempInsert.append('1')
            tempInsert.append('')
            tempInsert.append('0')
            tempInsert.append('0')
            tempInsert.append(str(insertime))
            
            mysqlcursor.execute("insert into ire_content_topn(contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , tempInsert)
            mysqlconn.commit()
            break
        if count>=20:
            break
        pycursor.close()
    outputDic['topn'] = topnlist
    outputstring = sj.dumps(outputDic)    
    fp.write(outputstring)
    fp.write(']')
        
        
mysqlcursor.close()
mysqlconn.close()

pyconn.close()




