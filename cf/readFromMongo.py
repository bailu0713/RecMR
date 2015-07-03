#coding:UTF-8
import pymongo
pyconn = pymongo.MongoClient(host='172.16.11.166',port=27017)
pycursor = pyconn.cibn.userid_cid_score.find().batch_size(30)
with open('/opt/IAE/cf/triple.txt','w') as fp:
    for i in pycursor:
        userId = i['userId']
        cid = i['cid']
        score = i['score']
        fp.write(str(userId)+','+str(cid)+','+str(score)+'\n')
