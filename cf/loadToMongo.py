import datetime
#coding:utf8
dateName=str(datetime.datetime.now())
dateName=dateName.split(" ")[0]
file=open("/ftp/CIBN/UserAction/CIBN_UserAction_%s.log"%dateName)
import pymongo
connection=pymongo.MongoClient(host="172.16.11.166",port=27017)
collection=connection.cibn.userid_cid_score.drop()
collection=connection.cibn.userid_cid_score
uid_cids = {}
uid = {}
for line in file:
    value = line.strip().split("|")
    if value[0] in uid_cids:
        if value[1] in uid_cids[value[0]]:
            uid_cids[value[0]][value[1]] += 1
        else:
            uid_cids[value[0]][value[1]] = 1
    else:
        uid_cids[value[0]] = {}
for user in uid_cids.keys():
    for cid in uid_cids[user].keys():
        if uid_cids[user][cid]>=3 and uid_cids[user][cid] <= 5:
            collection.insert_one({"userId":user,"cid":cid,"score":str(1.0)})
        elif uid_cids[user][cid] > 5 and uid_cids[user][cid] <= 10:
            collection.insert_one({"userId":user,"cid":cid,"score":str(2.0)})
        elif uid_cids[user][cid] > 10 and uid_cids[user][cid] <= 15:
            collection.insert_one({"userId":user,"cid":cid,"score":str(3.0)})
        elif uid_cids[user][cid] > 15 and uid_cids[user][cid] <= 20:
            collection.insert_one({"userId":user,"cid":cid,"score":str(4.0)})
        elif uid_cids[user][cid] > 20:
            collection.insert_one({"userId":user,"cid":cid,"score":str(5.0)})
