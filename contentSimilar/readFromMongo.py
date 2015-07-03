#coding:UTF-8
import pymongo
# from snowNlpTextRank import textRank

# textTool = textRank()

import MySQLdb
mysqlconn = MySQLdb.connect(host = '172.16.11.166', db = 'ire', charset = 'utf8')
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('select VALUE1 from system_parameter where CODE="directorlevel";')
directorlevel = mysqlcursor.fetchall()[0][0]
mysqlcursor.execute('select VALUE1 from system_parameter where CODE="keywordlevel";')
keywordlevel = mysqlcursor.fetchall()[0][0]
mysqlcursor.execute('select VALUE1 from system_parameter where CODE="actorlevel";')
actorlevel = mysqlcursor.fetchall()[0][0]
mysqlcursor.close()
mysqlconn.close()

with open('/opt/IAE/contentSimilar/contentSimilar.txt','w') as fp:
    pyconn = pymongo.MongoClient(host='172.16.11.166',port=27017)
    pycursor = pyconn.cibn.content_list.find().batch_size(30)
    for i in pycursor:
        cid = i['cid'].encode('utf8')
#         videoName = i['videoName'].encode('utf8')
#         videoBrief = i['videoBrief'].encode('utf8')
        keywords = i['keywords'].encode('utf8')
        year = i['year'].encode('utf8')
        classCode = i['classCode'].encode('utf8')
        actors = i['actors'].encode('utf8')
        if actors == '无' or actors=='' or actors=='N/A':
            actors = ''
        else:
            actors = ','.join(actors.split('|'))
        director = i['director'].encode('utf8')
        if director == '无' or director=='' or director=='N/A':
            director = ''
        else:
            director = ','.join(director.split('|'))
    #     temp = list()
    #     temp.append(userId)
    #     temp.append(cid)
    #     temp.append(score)
        if len(keywords)>0:
            fp.write(str(cid)+'||'+str(keywords)+'||'+str(year)+'||'+str(classCode)+'||'+str(actors)+'||'+str(director)+'||'+str(directorlevel)+'||'+str(keywordlevel)+'||'+str(actorlevel)+'\n')
#         if len(keywords)==0:
#             if len(videoBrief)>5:
#                 keywords = ''
#                 keywords = textTool.keywordRetrieval(videoBrief, 5)
#                 fp.write(str(cid)+'||'+str(keywords)+'||'+str(year)+'||'+str(classCode)+'||'+str(actors)+'||'+str(director)+'||'+str(directorlevel)+'||'+str(keywordlevel)+'||'+str(actorlevel)+'\n')
    
    pycursor.close()
    pyconn.close()


    

    
