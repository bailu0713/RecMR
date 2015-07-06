# coding:utf8

import sys

reload(sys)
sys.setdefaultencoding("utf8")
import datetime
import time
import simplejson as sj
import MySQLdb
import random

date = str(datetime.datetime.now()).split(" ")[0].replace("-", "")
liveRec = open("E:\\IRE\\huanEPG\\liveRec\\liveRec_%s.txt" % date, "w")

secondsPerMinute = 60
minutesPerHour = 60
hoursPerDay = 24
dayDuration = 15
totalRecNum=16

# tup = int(time.mktime(datetime.datetime.now().timetuple()))
tup = int(
    time.mktime(datetime.datetime.now().timetuple())) - secondsPerMinute * minutesPerHour * hoursPerDay * dayDuration
duration_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tup))
year = str(duration_time).split(".")[0].split(" ")[0].split("-")[0]
month = str(duration_time).split(".")[0].split(" ")[0].split("-")[1]
day = str(duration_time).split(".")[0].split(" ")[0].split("-")[2]
daytime = datetime.datetime(int(year), int(month), int(day))

connect = MySQLdb.connect(host='10.3.3.182', user='ire', passwd='ZAQ!XSW@CDE#', db='ire', charset='utf8')
cursor = connect.cursor()

def rec_cartoon_list():
    data = []
    tmpDic = dict()
    cursor.execute('select * from mediabestv where mtype="cartoon" ;')
    temp = cursor.fetchall()
    for doc in temp:
        tmpDic["name_cn"] = doc[2]
        tmpDic["mediaid"] = doc[0]
        tmpDic["index"] = doc[1]
        tmpDic["p_pic"] = doc[15]
        data.append(tmpDic.copy())
    return data


def rec_tv_list():
    data = []
    tmpDic = dict()
    cursor.execute('select * from mediabestv where mtype="tv" ;')
    temp = cursor.fetchall()
    for doc in temp:
        tmpDic["name_cn"] = doc[2]
        tmpDic["mediaid"] = doc[0]
        tmpDic["index"] = doc[1]
        tmpDic["p_pic"] = doc[15]
        data.append(tmpDic.copy())
    return data


def rec_movie_list():
    data = []
    tmpDic = dict()
    cursor.execute('select * from mediabestv where mtype="movie" ;')
    temp = cursor.fetchall()
    for doc in temp:
        tmpDic["name_cn"] = doc[2]
        tmpDic["mediaid"] = doc[0]
        tmpDic["index"] = doc[1]
        tmpDic["p_pic"] = doc[15]
        data.append(tmpDic.copy())
    return data


def rec_variety_list():
    data = []
    tmpDic = dict()
    cursor.execute('select * from mediabestv where mtype="variety" ;')
    temp = cursor.fetchall()
    for doc in temp:
        tmpDic["name_cn"] = doc[2]
        tmpDic["mediaid"] = doc[0]
        tmpDic["index"] = doc[1]
        tmpDic["p_pic"] = doc[15]
        data.append(tmpDic.copy())
    return data


tvlist = rec_tv_list()
cartoonlist = rec_cartoon_list()
movielist = rec_movie_list()
varietylist = rec_variety_list()


start = time.time()


def rec_list():
    tup = int(time.mktime(daytime.timetuple()))
    for i in range(0, 1440):
        database_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tup))
        tup += 60
        index = 0
        audiencerate_time = database_time.replace(":", "").replace("-", "").replace(" ", "")[8:12]
        for column in ["", "电视剧", "电影", "体育", "科教", "财经", "少儿", "娱乐", "综合"]:
            rec_list = list()
            rec_result = dict()
            if index == 0:
                index += 1
                cursor.execute(
                    'select a.channelId,a.channelName,a.programTitle from (select channelId,channelName,programTitle from livemedia where startTime<="%s" and endTime> "%s" and channelId <> "cctv1" and channelId<>"cctv5"and channelId <> "cctv6" and channelId<>"cctv3" and channelId <> "cctv8" and channelId<>"5927c7a6dd31f38686fafa073e2e13bc" and channelId<>"590e187a8799b1890175d25ec85ea352" and channelId<>"28502a1b6bf5fbe7c6da9241db596237" and channelId<>"9291c40ec1cec1281638720c74c7245f" and channelId<>"1ce026a774dba0d13dc0cef453248fb7" and channelId<>"5dfcaefe6e7203df9fbe61ffd64ed1c4" and channelId<>"23ab87816c24f90e5865116512e12c47" and channelId<>"20831bb807a45638cfaf81df1122024d" and channelId<>"55fc65ef82e92d0e1ccb2b3f200a7529" and channelId<>"c8bf387b1824053bdb0423ef806a2227" and channelId<>"c39a7a374d888bce3912df71bcb0d580" and channelId<>"6a3f44b1abfdfb49ddd051f9e683c86d" and channelId<>"dragontv" and channelId<>"322fa7b66243b8d0edef9d761a42f263" and channelId<>"antv"and wikiTitle<>"广告")a left join (select channel,tvRating,minute_time from audiencerate_min where minute_time= "%s" and date_time="2015-05-12")b on a.channelName=b.channel order by b.tvRating desc;' % (
                        database_time, database_time, audiencerate_time))
                result = cursor.fetchall()
                rec_result['time'] = database_time.replace(":", "").replace("-", "").replace(" ", "")[0:12]
                rec_result['column'] = 'all'
                if len(result) >= totalRecNum:
                    for doc in result:
                        rec_list.append(doc[0])
                    rec_result['rec_list'] = rec_list
                    liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                    liveRec.write("\n")
                elif len(result) >= 1 and len(result) < totalRecNum:
                    for doc in result:
                        rec_list.append(doc[0])
                    rec_result['rec_list'] = rec_list
                    rec_result['vod_list'] = random.sample(varietylist, totalRecNum - len(result))
                    liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                    liveRec.write("\n")
                else:
                    rec_result['rec_list'] = rec_list
                    rec_result['vod_list'] = random.sample(varietylist,totalRecNum)
                    liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                    liveRec.write("\n")
            else:
                cursor.execute(
                    'select a.channelId,a.channelName,a.programTitle from (select channelId,channelName,programTitle from livemedia where startTime<="%s" and endTime> "%s" and programType ="%s" and channelId <> "cctv1" and channelId<>"cctv5"and channelId <> "cctv6" and channelId<>"cctv3" and channelId <> "cctv8" and channelId<>"5927c7a6dd31f38686fafa073e2e13bc" and channelId<>"590e187a8799b1890175d25ec85ea352" and channelId<>"28502a1b6bf5fbe7c6da9241db596237" and channelId<>"9291c40ec1cec1281638720c74c7245f" and channelId<>"1ce026a774dba0d13dc0cef453248fb7" and channelId<>"5dfcaefe6e7203df9fbe61ffd64ed1c4" and channelId<>"23ab87816c24f90e5865116512e12c47" and channelId<>"20831bb807a45638cfaf81df1122024d" and channelId<>"55fc65ef82e92d0e1ccb2b3f200a7529" and channelId<>"c8bf387b1824053bdb0423ef806a2227" and channelId<>"c39a7a374d888bce3912df71bcb0d580" and channelId<>"6a3f44b1abfdfb49ddd051f9e683c86d" and channelId<>"dragontv" and channelId<>"322fa7b66243b8d0edef9d761a42f263" and channelId<>"antv"and wikiTitle<>"广告")a left join (select channel,tvRating,minute_time from audiencerate_min where minute_time= "%s" and date_time="2015-05-12")b on a.channelName=b.channel order by b.tvRating desc;' % (
                        database_time, database_time, column, audiencerate_time))
                result = cursor.fetchall()
                rec_result['time'] = database_time.replace(":", "").replace("-", "").replace(" ", "")[0:12]
                rec_result['column'] = column
                if column == "电视剧":
                    if len(result) >= totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    elif len(result) >= 1 and len(result) < totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(tvlist, totalRecNum - len(result))
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    else:
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(tvlist, totalRecNum)
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                elif column == "电影":
                    if len(result) >= totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    elif len(result) >= 1 and len(result) < totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(movielist, totalRecNum - len(result))
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    else:
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(movielist, totalRecNum)
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                elif column == "少儿":
                    if len(result) >= totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    elif len(result) >= 1 and len(result) < totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(cartoonlist, totalRecNum - len(result))
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    else:
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(cartoonlist, totalRecNum)
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                else:
                    if len(result) >= totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    elif len(result) >= 1 and len(result) < totalRecNum:
                        for doc in result:
                            rec_list.append(doc[0])
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(varietylist, totalRecNum - len(result))
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
                    else:
                        rec_result['rec_list'] = rec_list
                        rec_result['vod_list'] = random.sample(varietylist, totalRecNum)
                        liveRec.write(sj.dumps(rec_result, ensure_ascii=False))
                        liveRec.write("\n")
        print i


if __name__ == "__main__":
    rec_list()
