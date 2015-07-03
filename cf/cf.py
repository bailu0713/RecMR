#!/usr/bin/python
#coding:UTF-8
from mrjob.job import MRJob

class step(MRJob):
    '''
    '''
#     logging.c
    def parseMatrix(self, _, line):
        '''
        input one stdin for pymongo onetime search
        output contentId, (userId, rating)
        '''
        line = (str(line))
        line=line.split(',')
        userId = line[0]
#         print userId
        cid = line[1]
#         print cid
        score = float(line[2])
#         print score
        yield cid, (userId, float(score))        

    
    def scoreCombine(self, cid, userRating):
        '''
        将对同一个内容的（用户，评分）拼到一个list里
        '''
        userRatings = list()
        for i in userRating:
            userRatings.append(i)
        yield cid, userRatings
        
    def userBehavior(self, cid, userRatings):
        '''        
        '''
        scoreList = list()
        for doc in userRatings:
            # 每个combiner结果
            for i in doc:
                scoreList.append(i)
        for user1 in scoreList:
            for user2 in scoreList:
                if user1[0] == user2[0]:
                    continue
                yield (user1[0], user2[0]), (user1[1], user2[1])
    
    def calculateUserSimilarity(self, users, ratings):
        '''
        '''
        count = 0
        similarity = 0.0
        
        for user1R, user2R in ratings:
            count += 1
            similarity += pow(user1R-user2R,2)
        similarity = similarity/count
        yield users[0], (users[1], similarity,count)
        
    def processRecommendation(self, users, alternatives):
        '''
        input: "U20140418150743"    ["201311151748260011147", 0.5, 2]
        output "U20140430144028"    ["201310161654540011451", "20131022183026001195"]
        '''
        alternativeList = list()
        for targetUser, similarity, count in alternatives:
            temp = dict()
            temp['targetUser'] = targetUser
            temp['similarity'] = similarity
            temp['count'] = count
            alternativeList.append(temp)
        alternativeList = sorted(alternativeList, cmp=lambda x,y:cmp(x['similarity'],y['similarity']))
        listLength = len(alternativeList)
        # sort according to count if similarity identical
        while True:
            ifChanged = 0
            for i in range(listLength-1):
                if alternativeList[listLength-i-1]['similarity']==alternativeList[listLength-i-2]['similarity']:
                    if alternativeList[listLength-i-1]['count']>alternativeList[listLength-i-2]['count']:
                        temp = alternativeList[listLength-i-1]
                        alternativeList[listLength-i-1] = alternativeList[listLength-i-2]
                        alternativeList[listLength-i-2] = temp
                        ifChanged = 1
            if ifChanged == 0:
                break
                
        yield users, [user['targetUser'] for user in alternativeList]
    
    def steps(self):
        return [self.mr(mapper = self.parseMatrix,
                        reducer = self.scoreCombine),
                self.mr(reducer = self.userBehavior),
                self.mr(reducer = self.calculateUserSimilarity),
                self.mr(reducer = self.processRecommendation),]
#                 ]#self.mr(reducer = self.topNResult)]
    
    
if __name__=='__main__':
    
    step.run()