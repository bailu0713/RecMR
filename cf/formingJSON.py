#coding:UTF-8
import pymongo
import simplejson as sj
import MySQLdb
import datetime
import sys
reload(sys)
sys.setdefaultencoding("utf8")
class formingJSON():
    
    def topNResult(self, user, targetusers):
        '''
        input    user = "00000001"
                 targetusers = ["0001,"0002",...] 
        '''
        pyconn = pymongo.MongoClient(host='172.16.11.166',port=27017)
        pycursor = pyconn.cibn.userid_cid_score.find({'userId':user}).batch_size(30)
        userSeenList = list()
        for i in pycursor:
            userSeenList.append(i['cid'])
        pycursor.close()
        count = 0
        topTargetUser = 20
        topN = dict()
        for targetuser in targetusers:
            # user weight
            weight = 1-count*1.0/float(topTargetUser)
            pycursor = pyconn.cibn.userid_cid_score.find({'userId':targetuser[1:-1]}).batch_size(30)
            for doc in pycursor:
                if doc['score']>=3 and doc['cid'] not in userSeenList:
                    if doc['cid'] in topN:
                        topN[doc['cid']] += float(doc['score'])*weight
                    else:
                        topN[doc['cid']] = float(doc['score'])*weight
            count+=1
            
            if count>=topTargetUser:
                break
            pycursor.close()
        topN = sorted(topN.items(), key = lambda d:d[1], reverse = True)

        count = 0
        outputDic = dict()
        similarlist = list()
        
        insertList = list()
        for cid,cidscore in topN:
            tmpDict = dict()
            tmpDict['contentId'] = cid
            pycursor2 = pyconn.cibn.content_list.find({'cid':cid}).batch_size(30)
            if pycursor2.count()>0:
                for content in pycursor2:
                    tmpDict['videoName'] = content['videoName']
                    tmpDict['videoBrief'] = content['videoBrief']
                    tmpDict['actors'] = content['actors']
                    tmpDict['director'] = content['director']
                    tmpDict['classCode'] = content['classCode']
                    tmpDict['videoPic'] = content['videoPic']
                    break
            else:
                tmpDict['videoName']=''
                tmpDict['videoBrief'] = ''
                tmpDict['actors'] = ''
                tmpDict['director'] = ''
                tmpDict['classCode'] = ''
                tmpDict['videoPic'] = ''
            pycursor2.close()
            tmpDict['score'] = cidscore
#             cids.append(cid)
#             cidscores.append(cidscore)
            similarlist.append(tmpDict)
            count+=1
            if count>=10:
                break
        outputDic['reclist'] = similarlist
        user=user.replace('"','')
        user=user.replace('"','')
        outputDic['userId'] = user
        outputstring = sj.dumps(outputDic,ensure_ascii=False)
        #mysqlcursor.executemany("insert into ire_user_behav(userid, rec_id, rec_assetId, rec_name, rec_channelname, rec_pid, rec_pic, score, reason, systype, OSversion, main_type, create_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , (insertList))
        
        pyconn.close()
        fp.write(outputstring)
        fp.write('\n')
#         yield user, (cids,cidscores)
if __name__=='__main__':
    dateName = str(datetime.datetime.now())
    dateName=dateName.split(" ")[0]
    #nameindex = dateName.index('.')
    #dateName = dateName[:nameindex]
    #dateName = dateName.replace('-', '')
    #dateName = dateName.replace(':', '')
    #dateName = dateName.replace(' ', '')
    textName = '/ftp/CTVIT/behavior/cfResult'+dateName+'.txt'
    insertTime = datetime.datetime.now()
    mysqlconn = MySQLdb.connect(host = '172.16.11.166', db = 'ire', charset = 'utf8')
    mysqlcursor = mysqlconn.cursor()
    mysqlcursor.execute('TRUNCATE TABLE ire_user_behav;')
    commaCount = 0
    with open(textName,'w') as fp:
        fp.write('[')    
        readfp = open('/opt/IAE/cf/cfStep1.txt','r')
        for line in open('/opt/IAE/cf/cfStep1.txt'):            
            if commaCount==0:
                commaCount=1
            else:
                fp.write(',')
            line = readfp.readline()
            line = line.strip()
            content = line.split('\t')
            user = content[0]
            content[1] = content[1].replace('[','')
            content[1] = content[1].replace(']','')
            targetusers = content[1].split(', ')
            formingJSON().topNResult(user, targetusers)
        readfp.close()
        
        fp.write(']')
    mysqlcursor.close()
    mysqlconn.close()
    
