import datetime
dateName=str(datetime.datetime.now())
dateName=dateName.split(" ")[0]
file=open("/ftp/CIBN/Content/CIBN_Content_%s.log"%dateName)
import pymongo
connection=pymongo.MongoClient(host="172.16.11.166",port=27017)
collection=connection.cibn.content_list
for line in file:
    value=line.strip().split("|")
    collection.insert_one({"cid":value[0],"videoId":value[1],"videoName":value[2],"videoBrief":value[3],"videoPic":value[4], "director":value[5],"actors":value[6], "year":value[7],"area":value[8], "month":value[9], "classCode":value[10], "editor":value[11], "language":value[12], "firstletter":value[13], "videoLength":value[14], "keywords":value[15],"createDate":value[16]})
